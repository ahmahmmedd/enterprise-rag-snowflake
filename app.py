"""
============================================================================
 Enterprise Financial Document RAG Assistant
----------------------------------------------------------------------------
 Architecture:  Streamlit (UI)  ->  LangChain (LCEL orchestration)
                -> Snowflake Cortex Search (retrieval)
                -> Snowflake Cortex / Claude 3.5 Sonnet (generation)

 Data-flow summary:
   1. User types a question in the Streamlit chat box.
   2. The question is passed, unmodified, into an LCEL chain.
   3. The chain's `retriever` branch calls SnowflakeCortexSearchRetriever,
      which performs a REST call to a pre-existing Cortex Search service
      running INSIDE the Snowflake account boundary. The raw financial
      documents never leave Snowflake -- only the top-k retrieved text
      chunks are returned to this process.
   4. The chain's `question` branch simply forwards the raw question
      (RunnablePassthrough).
   5. Both branches are merged into a single dict {"context": ..., "question": ...}
      which is rendered by a ChatPromptTemplate into a grounded prompt.
   6. The rendered prompt is sent to ChatSnowflake (Claude 3.5 Sonnet served
      via Snowflake Cortex), which generates the final, context-grounded
      answer.
   7. The answer is streamed back to the Streamlit chat UI and appended to
      session-state chat history.

 Because steps 3-6 all execute against Snowflake-hosted services (Cortex
 Search + Cortex LLM functions), sensitive financial data is never sent to
 an external, non-Snowflake API endpoint -- it stays within the customer's
 Snowflake governance boundary (RBAC, masking policies, audit logs, etc.)
 the entire time.
============================================================================
"""

import os
import sys
import traceback

import streamlit as st
from dotenv import load_dotenv

# --- LangChain core (framework-agnostic LCEL primitives) -------------------
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser

# --- LangChain <> Snowflake partner package ---------------------------------
# create_session_from_env(): builds a snowflake.snowpark.Session from the
#   standard SNOWFLAKE_* environment variables (see .env.example).
# SnowflakeCortexSearchRetriever: LangChain Retriever backed by a Cortex
#   Search service (hybrid keyword + vector search, fully managed by
#   Snowflake -- no external vector DB required).
# ChatSnowflake: LangChain Chat Model that invokes Snowflake Cortex's
#   COMPLETE function, giving access to Claude 3.5 Sonnet (and other
#   Cortex-hosted models) without the data or the prompt ever leaving
#   Snowflake's network boundary.
from langchain_snowflake import (
    create_session_from_env,
    SnowflakeCortexSearchRetriever,
    ChatSnowflake,
)

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()  # populate os.environ from a local .env file, if present

APP_TITLE = "Enterprise Financial Document Assistant"
CORTEX_SEARCH_SERVICE = "FINANCE_DB.DATA.FINANCE_SEARCH"  # <db>.<schema>.<service>
CHAT_MODEL = os.getenv("CORTEX_CHAT_MODEL", "claude-3-5-sonnet")
RETRIEVER_K = 5
MODEL_TEMPERATURE = 0.1

# System prompt: the single most important guardrail in the whole app.
# It forces the LLM to ground every answer in the retrieved context instead
# of falling back on parametric knowledge -- critical for financial
# accuracy and auditability.
SYSTEM_PROMPT = """You are an expert financial research assistant embedded in a secure,
enterprise-grade Retrieval-Augmented Generation (RAG) system.

Your ONLY source of truth is the "Context" block below, which was retrieved
from the organization's governed financial document repository in Snowflake.

Rules you must follow at all times:
1. Answer strictly and exclusively using the information contained in the Context.
2. If the Context does not contain enough information to answer confidently,
   say so explicitly (e.g. "The retrieved documents do not contain this information.").
   Do NOT guess, speculate, or use outside/general knowledge.
3. Never fabricate figures, dates, company names, or citations.
4. When numbers, dates, or financial figures are present in the Context, quote
   them precisely as written.
5. Be concise, professional, and precise -- write as a financial analyst would.
6. Where helpful, reference which part of the context (e.g. document/section)
   supports your answer.

Context:
{context}
"""

# ============================================================================
# STREAMLIT PAGE CONFIG
# ============================================================================
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================================
# CACHED RESOURCE INITIALIZATION
# ============================================================================
# st.cache_resource ensures the Snowpark session, retriever, and LLM client
# are created ONCE per server process (not on every rerun / every user
# keystroke), which keeps the app fast and avoids exhausting Snowflake
# connection limits.
@st.cache_resource(show_spinner=False)
def get_snowflake_session():
    """
    Creates a Snowpark session from environment variables:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA (see .env.example).
    Credentials never touch application code -- they are injected purely
    through the environment, which is a secure-by-default pattern for
    enterprise deployments (Docker secrets, Kubernetes secrets, Streamlit
    Community Cloud secrets, etc.).
    """
    return create_session_from_env()


@st.cache_resource(show_spinner=False)
def get_retriever(_session):
    """
    Builds the Cortex Search retriever bound to the pre-existing
    FINANCE_DB.DATA.FINANCE_SEARCH service. All indexing, embedding, and
    similarity search happen server-side inside Snowflake -- this client
    only ever sees the top-k text chunks that come back.
    """
    return SnowflakeCortexSearchRetriever(
        session=_session,
        service_name=CORTEX_SEARCH_SERVICE,
        k=RETRIEVER_K,
    )


@st.cache_resource(show_spinner=False)
def get_llm(_session):
    """
    Instantiates Claude 3.5 Sonnet via Snowflake Cortex. Because the model
    is invoked through Cortex's COMPLETE function, inference happens inside
    Snowflake's secure perimeter -- no raw document text is sent to a
    third-party API outside the customer's Snowflake account.
    """
    return ChatSnowflake(
        session=_session,
        model=CHAT_MODEL,
        temperature=MODEL_TEMPERATURE,
    )


@st.cache_resource(show_spinner=False)
def build_rag_chain(_session):
    """
    Assembles the full LCEL pipeline:

        {"context": retriever, "question": RunnablePassthrough()}
            | prompt
            | llm
            | StrOutputParser()

    - The dict on the left runs both branches in parallel: the user's raw
      question is sent to the retriever (context) and passed through
      unchanged (question).
    - The merged dict feeds the ChatPromptTemplate, which renders the
      SYSTEM_PROMPT with the retrieved context injected, plus the user's
      original question as the human turn.
    - The rendered prompt goes to ChatSnowflake (Claude 3.5 Sonnet).
    - StrOutputParser extracts a plain string from the AIMessage response
      for easy rendering in Streamlit.
    """
    retriever = get_retriever(_session)
    llm = get_llm(_session)

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", "{question}"),
        ]
    )

    chain = (
        {"context": retriever, "question": RunnablePassthrough()}
        | prompt
        | llm
        | StrOutputParser()
    )
    return chain


# ============================================================================
# SIDEBAR -- CONFIGURATION & CONNECTION STATUS
# ============================================================================
def render_sidebar():
    with st.sidebar:
        st.title("⚙️ Configuration")
        st.markdown("---")

        st.subheader("Connection Status")

        # Lazily attempt the Snowflake connection so the app can render
        # even before credentials are verified, and so any auth errors
        # are shown cleanly in the UI instead of crashing the whole app.
        try:
            session = get_snowflake_session()
            account = session.get_current_account()
            warehouse = session.get_current_warehouse()
            role = session.get_current_role()
            st.success("Connected to Snowflake")
            st.markdown(f"**Account:** `{account}`")
            st.markdown(f"**Warehouse:** `{warehouse}`")
            st.markdown(f"**Role:** `{role}`")
        except Exception as exc:  # noqa: BLE001 - surface any auth/connection error to the user
            session = None
            st.error("Not connected to Snowflake")
            with st.expander("Show error details"):
                st.code(str(exc))

        st.markdown("---")
        st.subheader("RAG Pipeline Settings")
        st.markdown(f"**Search Service:**\n`{CORTEX_SEARCH_SERVICE}`")
        st.markdown(f"**Retriever k:** `{RETRIEVER_K}`")
        st.markdown(f"**Chat Model:** `{CHAT_MODEL}`")
        st.markdown(f"**Temperature:** `{MODEL_TEMPERATURE}`")

        st.markdown("---")
        st.subheader("Security Note")
        st.caption(
            "All retrieval and generation calls execute inside your "
            "Snowflake account boundary via Cortex Search and Cortex "
            "COMPLETE. Financial documents are never sent to an external "
            "vector store or third-party LLM API."
        )

        st.markdown("---")
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

        return session


# ============================================================================
# MAIN CHAT UI
# ============================================================================
def render_chat(session):
    st.title(f"📊 {APP_TITLE}")
    st.caption(
        "Ask questions about your financial documents. Answers are grounded "
        "exclusively in content retrieved from Snowflake Cortex Search."
    )

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Replay chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # New user input
    user_question = st.chat_input("Ask a question about your financial documents...")

    if user_question:
        if session is None:
            st.error(
                "Cannot process your question: no active Snowflake session. "
                "Check your credentials in the sidebar / .env file."
            )
            return

        # Show and store the user's message
        st.session_state.messages.append({"role": "user", "content": user_question})
        with st.chat_message("user"):
            st.markdown(user_question)

        # Run the RAG chain and stream the answer
        with st.chat_message("assistant"):
            with st.spinner("Searching financial documents and generating answer..."):
                try:
                    rag_chain = build_rag_chain(session)
                    answer = rag_chain.invoke(user_question)
                except Exception as exc:  # noqa: BLE001
                    answer = (
                        "⚠️ An error occurred while processing your question. "
                        f"Details: `{exc}`"
                    )
                    traceback.print_exc(file=sys.stderr)

            st.markdown(answer)

        st.session_state.messages.append({"role": "assistant", "content": answer})


# ============================================================================
# ENTRYPOINT
# ============================================================================
def main():
    session = render_sidebar()
    render_chat(session)


if __name__ == "__main__":
    main()
