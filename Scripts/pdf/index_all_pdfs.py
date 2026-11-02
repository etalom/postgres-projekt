from pathlib import Path
import sys

from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "dags"))
from pdf_rag_config import CHUNK_OVERLAP, CHUNK_SIZE

DATABASE_URL = (
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

PDF_DIR = Path("../Data/pdfs")

engine = create_engine(DATABASE_URL)

splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP
)

embeddings = OllamaEmbeddings(
    model="nomic-embed-text"
)

pdf_files = list(PDF_DIR.glob("*.pdf"))

print(f"{len(pdf_files)} PDFs gefunden")

stored = 0

for pdf_file in pdf_files:
    print(f"\nVerarbeite: {pdf_file.name}")

    loader = PyPDFLoader(str(pdf_file))
    pages = loader.load()
    chunks = splitter.split_documents(pages)
    print(f"Chunks: {len(chunks)}")

    if not chunks:
        print(f"Überspringe {pdf_file.name}: keine Text-Chunks gefunden")
        continue

    vectors = embeddings.embed_documents(
        [chunk.page_content for chunk in chunks]
    )
    if len(vectors) != len(chunks):
        raise ValueError(
            f"Embedding-Anzahl für {pdf_file.name} stimmt nicht mit der Chunk-Anzahl überein"
        )

    with engine.begin() as conn:
        conn.execute(
            text("""
                DELETE FROM pdf_embeddings
                WHERE source_file = :source_file
            """),
            {"source_file": pdf_file.name}
        )

        conn.execute(
            text("""
                INSERT INTO pdf_embeddings
                (
                    source_file,
                    chunk_number,
                    page_number,
                    chunk_text,
                    embedding
                )
                VALUES
                (
                    :source_file,
                    :chunk_number,
                    :page_number,
                    :chunk_text,
                    CAST(:embedding AS vector)
                )
            """),
            [
                {
                    "source_file": pdf_file.name,
                    "chunk_number": chunk_number,
                    "page_number": int(chunk.metadata["page"]) + 1,
                    "chunk_text": chunk.page_content,
                    "embedding": str(vector),
                }
                for chunk_number, (chunk, vector) in enumerate(
                    zip(chunks, vectors, strict=True)
                )
            ]
        )

    stored += len(chunks)

print("\nFertig!")
print(f"Gespeicherte Chunks: {stored}")