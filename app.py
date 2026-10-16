"""
============================================================================
 Enterprise Financial Document RAG Assistant
============================================================================
"""

import os
import sys
import traceback
import streamlit as st
from dotenv import load_dotenv

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_snowflake import (
    SnowflakeCortexSearchRetriever,
    ChatSnowflake,
)
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

# ============================================================================
# CONFIGURATION
# ============================================================================
load_dotenv()

APP_TITLE = "Financial Intelligence"
CORTEX_SEARCH_SERVICE = "FINANCE_DB.DATA.FINANCE_SEARCH"
CHAT_MODEL = os.getenv("CORTEX_CHAT_MODEL", "claude-3-5-sonnet")
RETRIEVER_K = 5
MODEL_TEMPERATURE = 0.1

SYSTEM_PROMPT = """You are an expert financial research assistant embedded in a secure,
enterprise-grade Retrieval-Augmented Generation (RAG) system.

Your ONLY source of truth is the "Context" block below. 
Answer strictly and exclusively using the information contained in the Context.
Be concise, professional, and precise.

Context:
{context}
"""

# ============================================================================
# STREAMLIT PAGE CONFIG & AGENCY CSS
# ============================================================================
st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
    initial_sidebar_state="expanded",
)

def inject_agency_css():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Fraunces:ital,opsz,wght@0,9..144,300..700;1,9..144,300..700&family=Geist:wght@300;400;500;600;700&family=Geist+Mono:wght@400;500&display=swap');

        /* Global Font & Background Override */
        html, body, [data-testid="stAppViewContainer"] {
            font-family: 'Geist', sans-serif;
            background-color: #06060D !important;
            color: #F2EFEA !important;
        }
        
        /* Protect Streamlit Icons from the font override */
        .stIcon, .material-symbols-rounded, [data-testid="stSidebarCollapseButton"] *, [data-testid="collapsedControl"] * {
            font-family: 'Material Symbols Rounded' !important;
        }
        
        /* Hide Streamlit Clutter (Safely) */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* Keep the header functional but transparent, and explicitly color the toggle icon */
        header {
            background: transparent !important;
        }
        [data-testid="stSidebarCollapsedControl"] svg,
        [data-testid="collapsedControl"] svg, 
        .st-emotion-cache-1vt4ygl svg {
            color: #F2EFEA !important;
            fill: #F2EFEA !important;
        }
        
        /* Add a subtle background to the toggle button so it stands out against the dark void */
        [data-testid="collapsedControl"], 
        [data-testid="stSidebarCollapsedControl"] {
            background: rgba(20, 20, 28, 0.8) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            border-radius: 8px !important;
            margin-top: 8px !important;
            margin-left: 8px !important;
            visibility: visible !important;
        }

        /* Agency Typography */
        .eyebrow {
            display: inline-flex;
            align-items: center;
            font-family: 'Geist Mono', monospace;
            font-size: 11px;
            letter-spacing: .18em;
            text-transform: uppercase;
            color: #8A8694;
            padding: 6px 14px;
            border: 1px solid rgba(255,255,255,.06);
            border-radius: 100px;
            margin-bottom: 24px;
            background: rgba(255,255,255,.02);
            backdrop-filter: blur(8px);
        }
        .s-title {
            font-family: 'Fraunces', Georgia, serif;
            font-weight: 300;
            font-size: clamp(36px, 4vw, 56px);
            line-height: 1.05;
            letter-spacing: -.03em;
            margin-top: 0;
            padding-top: 0;
            margin-bottom: 16px;
        }
        .s-title em {
            font-style: italic;
            font-weight: 300;
            background: linear-gradient(90deg, #B45AFF 0%, #E095FF 50%, #B45AFF 100%);
            background-size: 200% 100%;
            -webkit-background-clip: text;
            background-clip: text;
            -webkit-text-fill-color: transparent;
            animation: textFlow 8s linear infinite;
        }
        @keyframes textFlow { to { background-position: 200% 0; } }
        
        .lead {
            font-size: 17px;
            line-height: 1.6;
            color: #8A8694;
            max-width: 60ch;
            margin-bottom: 40px;
        }
        
        /* Sidebar Restyling */
        [data-testid="stSidebar"] {
            border-right: 1px solid rgba(255,255,255,.04) !important;
            background: #0A0A12 !important;
        }
        .sidebar-header {
            font-family: 'Geist Mono', monospace;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: .15em;
            color: #5A5664;
            margin-top: 2rem;
            margin-bottom: 1rem;
            border-bottom: 1px solid rgba(255,255,255,.04);
            padding-bottom: 8px;
        }
        .metric-value {
            font-family: 'Geist Mono', monospace;
            color: #B45AFF;
            font-size: 13px;
            margin-top: 4px;
            margin-bottom: 16px;
            display: block;
        }
        
        /* Chat UI Restyling */
        [data-testid="stChatMessage"] {
            background-color: #0A0A12 !important;
            border: 1px solid rgba(255,255,255,.04) !important;
            border-radius: 16px !important;
            padding: 24px !important;
            margin-bottom: 16px !important;
        }
        [data-testid="stChatMessageAvatar"] {
            background-color: #14141C !important;
            color: #B45AFF !important;
            border: 1px solid rgba(180,90,255,.2) !important;
            border-radius: 8px !important;
            font-family: 'Geist Mono', monospace !important;
            font-size: 12px !important;
        }
        
        /* Input Box */
        [data-testid="stChatInput"] {
            background-color: #0A0A12 !important;
            border: 1px solid rgba(255,255,255,.08) !important;
            border-radius: 12px !important;
        }
        
        /* Buttons */
        .stButton > button {
            background: transparent !important;
            color: #F2EFEA !important;
            border-radius: 100px !important;
            font-family: 'Geist Mono', monospace !important;
            font-size: 12px !important;
            letter-spacing: 0.05em !important;
            text-transform: uppercase !important;
            border: 1px solid rgba(255,255,255,.1) !important;
            transition: all 220ms ease !important;
        }
        .stButton > button:hover {
            border-color: #B45AFF !important;
            color: #B45AFF !important;
            background: rgba(180,90,255,.04) !important;
        }
        </style>
    """, unsafe_allow_html=True)


# ============================================================================
# CACHED RESOURCE INITIALIZATION
# ============================================================================
@st.cache_resource(show_spinner=False)
def get_snowflake_session():
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "rsa_key.p8")
    with open(key_path, "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "private_key": pkb,
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    return Session.builder.configs(connection_params).create()

@st.cache_resource(show_spinner=False)
@st.cache_resource(show_spinner=False)
@st.cache_resource(show_spinner=False)
def get_retriever(_session):
    return SnowflakeCortexSearchRetriever(
        session=_session,
        service_name=CORTEX_SEARCH_SERVICE,
        k=RETRIEVER_K,
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        text_column="chunk_text"
    )

@st.cache_resource(show_spinner=False)
def get_llm(_session):
    return ChatSnowflake(
        session=_session,
        model=CHAT_MODEL,
        temperature=MODEL_TEMPERATURE,
    )

@st.cache_resource(show_spinner=False)
def build_rag_chain(_session):
    retriever = get_retriever(_session)
    llm = get_llm(_session)
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{question}"),
    ])
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
        st.markdown("<div class='sidebar-header'>System Architecture</div>", unsafe_allow_html=True)

        try:
            session = get_snowflake_session()
            account = session.get_current_account()
            role = session.get_current_role()
            st.markdown(f"Account<br><span class='metric-value'>{account}</span>", unsafe_allow_html=True)
            st.markdown(f"Active Role<br><span class='metric-value'>{role}</span>", unsafe_allow_html=True)
            st.success("Connection Active")
        except Exception as exc:
            session = None
            st.error("Connection Failed")
            with st.expander("View Exception Details"):
                st.code(str(exc))

        st.markdown("<div class='sidebar-header'>Engine Parameters</div>", unsafe_allow_html=True)
        st.markdown(f"Search Service<br><span class='metric-value'>{CORTEX_SEARCH_SERVICE.split('.')[-1]}</span>", unsafe_allow_html=True)
        st.markdown("Inference Model<br><span class='metric-value'>claude-3.5-sonnet</span>", unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Initialize New Session", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

        return session


# ============================================================================
# MAIN CHAT UI
# ============================================================================
# ============================================================================
# MAIN CHAT UI
# ============================================================================
# ============================================================================
# MAIN CHAT UI
# ============================================================================
def render_chat(session):
    st.markdown("<span class='eyebrow'>Enterprise RAG Interface</span>", unsafe_allow_html=True)
    st.markdown("<h1 class='s-title'>Financial intelligence, <br><em>built to compound.</em></h1>", unsafe_allow_html=True)
    st.markdown("<p class='lead'>Strict zero-trust retrieval running entirely within the warehouse boundary via Snowflake Cortex Search and Claude 3.5 Sonnet.</p>", unsafe_allow_html=True)

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Replay chat history (avatar parameter removed)
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    user_question = st.chat_input("Query enterprise financial documents...")

    if user_question:
        if session is None:
            st.error("Authentication required. Verify Key-Pair credentials.")
            return

        # Show user message (avatar parameter removed)
        st.session_state.messages.append({"role": "user", "content": user_question})
        with st.chat_message("user"):
            st.markdown(user_question)

        # Generate and show assistant message (avatar parameter removed)
        with st.chat_message("assistant"):
            with st.spinner("Executing retrieval..."):
                try:
                    rag_chain = build_rag_chain(session)
                    answer = rag_chain.invoke(user_question)
                except Exception as exc:
                    answer = f"[ System Exception ]\n\n`{exc}`"
                    traceback.print_exc(file=sys.stderr)

            st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})


# ============================================================================
# ENTRYPOINT
# ============================================================================
def main():
    inject_agency_css()
    session = render_sidebar()
    render_chat(session)

if __name__ == "__main__":
    main()