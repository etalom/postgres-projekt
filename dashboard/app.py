from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import ollama

from sqlalchemy import create_engine, text
from langchain_ollama import OllamaEmbeddings

# ==================================================
# FastAPI
# ==================================================

app = FastAPI(
    title="Vergaberecht RAG API",
    description="Hybrid RAG mit PostgreSQL, pgvector und Ollama",
    version="1.0.0"
)

# ==================================================
# Datenbank
# ==================================================

DATABASE_URL = (
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

engine = create_engine(DATABASE_URL)

# ==================================================
# Embeddings
# ==================================================

embeddings = OllamaEmbeddings(
    model="nomic-embed-text"
)

# ==================================================
# Request Modell
# ==================================================

class ChatRequest(BaseModel):
    question: str
    top_k: int = 5
    temperature: float = 0.1
    model: str = "llama3.2"

# ==================================================
# Root Endpoint
# ==================================================

@app.get("/")
def root():

    return {
        "status": "running",
        "service": "RAG API",
        "database": "PostgreSQL",
        "llm": "Ollama"
    }

# ==================================================
# Health Check
# ==================================================

@app.get("/health")
def health():

    try:

        with engine.connect() as conn:

            conn.execute(
                text("SELECT 1")
            )

        return {
            "status": "healthy"
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

# ==================================================
# Chat Endpoint
# ==================================================

@app.post("/chat")
def chat(request: ChatRequest):

    try:

        # ------------------------------------------
        # User Query Embedding
        # ------------------------------------------

        query_vector = embeddings.embed_query(
            request.question
        )

        with engine.connect() as conn:

            # --------------------------------------
            # Vector Search
            # --------------------------------------

            vector_result = conn.execute(
                text("""
                    SELECT
                        source_file,
                        chunk_number,
                        chunk_text,
                        1 - (
                            embedding <=>
                            CAST(:vector AS vector)
                        ) AS score
                    FROM pdf_embeddings
                    ORDER BY embedding <=>
                             CAST(:vector AS vector)
                    LIMIT :top_k
                """),
                {
                    "vector": str(query_vector),
                    "top_k": request.top_k
                }
            )

            vector_rows = vector_result.fetchall()

            # --------------------------------------
            # Full Text Search
            # --------------------------------------

            keyword_result = conn.execute(
                text("""
                    SELECT
                        source_file,
                        chunk_number,
                        chunk_text,
                        ts_rank(
                            search_vector,
                            plainto_tsquery(
                                'german',
                                :frage
                            )
                        ) AS score
                    FROM pdf_embeddings
                    WHERE search_vector @@
                          plainto_tsquery(
                              'german',
                              :frage
                          )
                    ORDER BY score DESC
                    LIMIT :top_k
                """),
                {
                    "frage": request.question,
                    "top_k": request.top_k
                }
            )

            keyword_rows = keyword_result.fetchall()

        # ------------------------------------------
        # Hybrid Ranking
        # ------------------------------------------

        docs = {}

        for row in vector_rows:

            key = (
                row.source_file,
                row.chunk_number
            )

            docs[key] = {
                "source_file": row.source_file,
                "chunk_number": row.chunk_number,
                "chunk_text": row.chunk_text,
                "score": row.score * 0.7
            }

        for row in keyword_rows:

            key = (
                row.source_file,
                row.chunk_number
            )

            if key in docs:

                docs[key]["score"] += (
                    row.score * 0.3
                )

            else:

                docs[key] = {
                    "source_file": row.source_file,
                    "chunk_number": row.chunk_number,
                    "chunk_text": row.chunk_text,
                    "score": row.score * 0.3
                }

        hybrid_docs = sorted(
            docs.values(),
            key=lambda x: x["score"],
            reverse=True
        )[:request.top_k]

        # ------------------------------------------
        # Kontext erzeugen
        # ------------------------------------------

        context = "\n\n".join(
            doc["chunk_text"]
            for doc in hybrid_docs
        )

        # ------------------------------------------
        # LLM
        # ------------------------------------------

        response = ollama.chat(
            model=request.model,
            messages=[
                {
                    "role": "system",
                    "content": """
Du bist ein Experte für Vergaberecht.

Nutze ausschließlich die bereitgestellten Dokumente.

Wenn die Antwort nicht im Kontext steht,
sage ausdrücklich:

'Diese Information konnte in den
bereitgestellten Dokumenten
nicht gefunden werden.'
"""
                },
                {
                    "role": "user",
                    "content": f"""
Kontext:

{context}

Frage:

{request.question}
"""
                }
            ],
            options={
                "temperature": request.temperature,
                "top_p": 0.9,
                "repeat_penalty": 1.1,
                "num_predict": 1500
            }
        )

        answer = response["message"]["content"]

        return {
            "question": request.question,
            "answer": answer,
            "documents_found": len(hybrid_docs),
            "sources": [
                {
                    "file": doc["source_file"],
                    "chunk": doc["chunk_number"],
                    "score": round(
                        doc["score"],
                        4
                    )
                }
                for doc in hybrid_docs
            ]
        }

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        )