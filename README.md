Enterprise Financial Document RAG Assistant

**A secure, in-warehouse Retrieval-Augmented Generation (RAG) application for financial document Q&A — built with Streamlit, LangChain (LCEL), Snowflake Cortex Search, and Claude 3.5 Sonnet.**

---

## Why This Architecture Matters

Most GenAI chatbot tutorials wire an LLM directly to a third-party vector database, meaning sensitive enterprise data — in this case, financial documents — has to leave the data warehouse to be embedded, indexed, and retrieved. For regulated industries (finance, healthcare, public sector), that's a non-starter.

This project takes a different approach: **the entire retrieval-and-generation pipeline executes inside the Snowflake account boundary.**

| Stage | Where it runs | What leaves Snowflake |
|---|---|---|
| Document storage | Snowflake tables/stages | Nothing |
| Embedding & indexing | Snowflake Cortex Search (managed service) | Nothing |
| Similarity / hybrid search | Snowflake Cortex Search | Only the top-*k* retrieved text chunks |
| LLM inference (Claude 3.5 Sonnet) | Snowflake Cortex `COMPLETE` | Only the rendered prompt + generated answer |
| Orchestration | LangChain (LCEL), local process | — |
| UI | Streamlit, local/cloud process | — |

Because Cortex Search and Cortex LLM functions run **as native Snowflake compute**, this pattern:

- **Keeps data within the governance boundary** — Snowflake's existing RBAC, row/column masking policies, network policies, and audit logging apply automatically to every retrieval and generation call. No data replication into an external vector store.
- **Supports data residency and compliance requirements** (e.g. GDPR, SOC 2, financial services regulations) by avoiding a separate, ungoverned copy of sensitive text embeddings living outside the warehouse.
- **Eliminates a whole class of infrastructure** — no standalone vector database, no separate embedding pipeline, no additional data-sync jobs to keep an external index fresh.
- **Bridges traditional ETL and GenAI** — the same Snowflake pipelines, warehouses, and roles that already govern financial data now also power semantic search and LLM-based reasoning over that data, rather than bolting on a disconnected AI stack.

In short: this is what "Retrieval-Augmented Generation" looks like when a Data Engineer, not just an ML Engineer, designs the system.

---

## Architecture Diagram

```
┌──────────────────┐        ┌───────────────────────────────────────────────────────┐
│                   │        │                     SNOWFLAKE ACCOUNT                  │
│    Streamlit UI   │        │                                                         │
│  (chat interface) │        │   ┌───────────────────┐        ┌────────────────────┐   │
│                   │ Q      │   │  Cortex Search     │        │   Cortex COMPLETE   │   │
│  st.chat_input ───┼────────┼──▶│  FINANCE_DB.DATA.  │        │  (Claude 3.5 Sonnet)│   │
│                   │        │   │  FINANCE_SEARCH    │        │                     │   │
│                   │        │   └─────────┬──────────┘        └──────────▲──────────┘   │
│                   │        │             │ top-k chunks                 │ prompt       │
│                   │        │             ▼                              │              │
│                   │        │      ┌───────────────────────────────────────────┐        │
│                   │        │      │      LangChain LCEL Chain (this app)       │        │
│                   │        │      │  {"context": retriever,                    │        │
│                   │        │      │   "question": passthrough}                 │        │
│                   │        │      │        | ChatPromptTemplate                │        │
│                   │        │      │        | ChatSnowflake(Claude 3.5 Sonnet)  │        │
│                   │        │      └───────────────────┬───────────────────────┘        │
│  st.chat_message  │ answer │                          │ answer                          │
│  ◀─────────────────────────┼──────────────────────────┘                                │
└──────────────────┘        │                                                            │
                             │   Financial Documents (tables / stages) — never leave here │
                             └───────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | [Streamlit](https://streamlit.io/) |
| Orchestration | [LangChain](https://python.langchain.com/) Core + LCEL |
| Snowflake Integration | [`langchain-snowflake`](https://github.com/langchain-ai/langchain-snowflake) (official partner package) |
| Retrieval | Snowflake Cortex Search (`SnowflakeCortexSearchRetriever`) |
| Generation | Claude 3.5 Sonnet via Snowflake Cortex (`ChatSnowflake`) |
| Session/Auth | `create_session_from_env()` — environment-variable based Snowpark session |

---

## Project Structure

```
.
├── app.py               # Main Streamlit application (RAG chain + chat UI)
├── requirements.txt      # Python dependencies
├── .env.example          # Template for Snowflake credentials
└── README.md              # You are here
```

---

## Prerequisites

Before running this app, you need a Snowflake account with:

1. **Cortex features enabled** on your account/region.
2. Financial documents loaded and chunked into a table.
3. A **Cortex Search Service** already created over that table, named:
   ```
   FINANCE_DB.DATA.FINANCE_SEARCH
   ```
   (Update `CORTEX_SEARCH_SERVICE` in `app.py` if your service uses a different fully-qualified name.)

   Example service creation (run in Snowsight / SQL):
   ```sql
   CREATE OR REPLACE CORTEX SEARCH SERVICE FINANCE_DB.DATA.FINANCE_SEARCH
     ON chunk_text
     ATTRIBUTES document_name, section
     WAREHOUSE = FINANCE_WH
     TARGET_LAG = '1 hour'
     AS (
       SELECT chunk_text, document_name, section
       FROM FINANCE_DB.DATA.FINANCIAL_DOCUMENT_CHUNKS
     );
   ```
4. A role/warehouse combination with `USAGE` on the Cortex Search service and access to Cortex LLM functions.
5. Access to Claude 3.5 Sonnet via Snowflake Cortex in your account's region (cross-region inference may need to be enabled).

---

## Setup & Installation

### 1. Clone and create a virtual environment

```bash
git clone <your-repo-url>
cd enterprise-rag-snowflake
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Snowflake credentials

```bash
cp .env.example .env
```

Edit `.env` with your Snowflake account identifier, username, password, role, warehouse, database, and schema. `create_session_from_env()` reads these automatically at runtime — **credentials never appear in application code.**

### 4. Run the app

```bash
streamlit run app.py
```

Open the URL Streamlit prints (typically `http://localhost:8501`).

---

## How the RAG Chain Works (LCEL)

The retrieval-augmented generation logic is expressed declaratively using LangChain Expression Language:

```python
rag_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | llm
    | StrOutputParser()
)
```

1. **Parallel fan-out**: the user's question is simultaneously (a) sent to `SnowflakeCortexSearchRetriever` to fetch the top 5 most relevant chunks from `FINANCE_DB.DATA.FINANCE_SEARCH`, and (b) passed through unchanged.
2. **Prompt assembly**: `ChatPromptTemplate` injects the retrieved context and the original question into a system/human message pair that instructs the model to answer **only** from the provided context.
3. **Generation**: `ChatSnowflake` sends the rendered prompt to Claude 3.5 Sonnet through Snowflake Cortex's `COMPLETE` function and returns a grounded answer.
4. **Parsing**: `StrOutputParser` extracts a clean string for display in the Streamlit chat UI.

This is the same LCEL pattern used in production LangChain RAG systems — just pointed at Snowflake-native retrieval and generation instead of external services.

---

## Security & Governance Highlights

- **No external vector database.** Embeddings and the search index are fully managed inside Snowflake Cortex Search.
- **No credentials in code.** All Snowflake auth is environment-variable driven via `create_session_from_env()`.
- **Inherits Snowflake RBAC.** Access to the underlying financial documents is governed by the same roles, grants, and masking policies already enforced on the source tables.
- **Auditable.** Every Cortex Search and Cortex COMPLETE call is logged within Snowflake, supporting compliance and audit requirements common in financial services.
- **Grounded answers only.** The system prompt explicitly forbids the model from answering outside the retrieved context, reducing hallucination risk on financial figures.

---

## Extending This Project

- **Add conversation memory** so follow-up questions can reference prior turns (e.g. `RunnableWithMessageHistory`).
- **Add filters** (e.g. by fiscal year, document type) using `SnowflakeCortexSearchRetriever`'s metadata filtering.
- **Add citations** by having the retriever return source document names/sections and rendering them alongside the answer.
- **Add evaluation** using Snowflake's AI Observability / TruLens integration to track groundedness and answer relevance over time.
- **Deploy** as a Streamlit-in-Snowflake (SiS) app for an even tighter security boundary, or containerize with Docker for cloud deployment.

---

## License

MIT — feel free to fork and adapt for your own portfolio or enterprise use case.
