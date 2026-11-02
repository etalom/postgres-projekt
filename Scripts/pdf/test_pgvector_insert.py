from sqlalchemy import create_engine, text
from langchain_ollama import OllamaEmbeddings

DATABASE_URL = (
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

engine = create_engine(DATABASE_URL)

embeddings = OllamaEmbeddings(
    model="nomic-embed-text"
)

content = """
Das ist mein erster Test für
PostgreSQL und pgvector.
"""

vector = embeddings.embed_query(content)

with engine.begin() as conn:

    conn.execute(
        text("""
            INSERT INTO pdf_embeddings
            (
                source_file,
                chunk_number,
                chunk_text,
                embedding
            )
            VALUES
            (
                :source_file,
                :chunk_number,
                :chunk_text,
                CAST(:embedding AS vector)
            )
        """),
        {
            "source_file": "test.pdf",
            "chunk_number": 0,
            "chunk_text": content,
            "embedding": str(vector)
        }
    )

print("Embedding gespeichert.")