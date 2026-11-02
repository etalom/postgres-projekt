import ollama

from langchain_ollama import OllamaEmbeddings
from sqlalchemy import create_engine, text

DATABASE_URL = (
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

engine = create_engine(DATABASE_URL)

frage = input("Frage: ")

embeddings = OllamaEmbeddings(
    model="nomic-embed-text"
)

query_vector = embeddings.embed_query(
    frage
)

with engine.connect() as conn:

    result = conn.execute(
        text("""
            SELECT
                source_file,
                chunk_text
            FROM pdf_embeddings
            ORDER BY embedding <=> CAST(:vector AS vector)
            LIMIT 5
        """),
        {
            "vector": str(query_vector)
        }
    )

    rows = result.fetchall()

kontext = "\n\n".join(
    row[1]
    for row in rows
)

antwort = ollama.chat(
    model="llama3.2",
    messages=[
        {
            "role": "user",
            "content": f"""
Beantworte die Frage ausschließlich auf Basis des folgenden Kontexts.

Kontext:
{kontext}

Frage:
{frage}

Wenn die Information nicht im Kontext enthalten ist,
dann sage dies ausdrücklich.
"""
        }
    ]
)

print("\n" + "=" * 80)
print("ANTWORT")
print("=" * 80)
print(
    antwort["message"]["content"]
)