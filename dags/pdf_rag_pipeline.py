from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

from sqlalchemy import create_engine, text

from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings
from airflow.datasets import Dataset
from pdf_rag_config import CHUNK_OVERLAP, CHUNK_SIZE
# ---------------------------------------------------
# Konfiguration
# ---------------------------------------------------

DATABASE_URL = (
    "postgresql+psycopg2://"
    "admin:Admi123!@postgres:5432/meinedb"
)

PDF_FOLDER = (
    "/opt/airflow/data/incoming_pdfs"
)

OLLAMA_URL = (
    "http://172.17.0.1:11434"
)
# ---------------------------------------------------
# Datasets
# ---------------------------------------------------

raw_dataset = Dataset(
    "postgres://meinedb/public/raw_csv_loaded"
)

pdf_embeddings_dataset = Dataset(
    "postgres://meinedb/public/pdf_embeddings"
)

# ---------------------------------------------------
# Neue PDFs finden
# ---------------------------------------------------
import os

def get_uploaded_documents():

    engine = create_engine(
        DATABASE_URL
    )

    pdf_files = [
        f
        for f in os.listdir(PDF_FOLDER)
        if f.lower().endswith(".pdf")
    ]

    with engine.begin() as conn:

        for filename in pdf_files:

            conn.execute(
                text("""
                    INSERT INTO documents
                    (
                        filename,
                        status
                    )
                    VALUES
                    (
                        :filename,
                        'uploaded'
                    )
                    ON CONFLICT (filename)
                    DO NOTHING
                """),
                {
                    "filename": filename
                }
            )

    with engine.connect() as conn:

        result = conn.execute(
            text("""
                SELECT filename
                FROM documents
                WHERE status='uploaded'
            """)
        )

        files = [
            row[0]
            for row in result
        ]

    print(
        f"Gefundene PDFs: {files}"
    )

    return files

# ---------------------------------------------------
# Status auf processing setzen
# ---------------------------------------------------

def mark_processing(**context):

    files = context["ti"].xcom_pull(
        task_ids="get_uploaded_documents"
    )

    if not files:

        print(
            "Keine PDFs gefunden"
        )

        return

    engine = create_engine(
        DATABASE_URL
    )

    with engine.begin() as conn:

        for filename in files:

            conn.execute(
                text("""
                    UPDATE documents
                    SET status = 'processing'
                    WHERE filename = :filename
                """),
                {
                    "filename": filename
                }
            )

            print(
                f"{filename} -> processing"
            )


# ---------------------------------------------------
# PDF lesen
# ---------------------------------------------------

def extract_text():

    engine = create_engine(
        DATABASE_URL
    )

    with engine.connect() as conn:

        result = conn.execute(
            text("""
                SELECT filename
                FROM documents
                WHERE status = 'processing'
            """)
        )

        files = [
            row[0]
            for row in result
        ]

    extracted_documents = []

    for filename in files:

        pdf_path = (
            f"{PDF_FOLDER}/{filename}"
        )

        print(
            f"Lese Datei: {pdf_path}"
        )

        loader = PyPDFLoader(
            pdf_path
        )

        pages = loader.load()

        print(
            f"{filename}: "
            f"{len(pages)} Seiten"
        )

        extracted_documents.append(
            {
                "filename": filename,
                "pages": len(pages)
            }
        )

    return extracted_documents


# ---------------------------------------------------
# Chunking
# ---------------------------------------------------

def chunk_documents():

    engine = create_engine(
        DATABASE_URL
    )

    with engine.connect() as conn:

        result = conn.execute(
            text("""
                SELECT filename
                FROM documents
                WHERE status = 'processing'
            """)
        )

        files = [
            row[0]
            for row in result
        ]

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP
    )

    total_chunks = 0

    for filename in files:

        pdf_path = (
            f"{PDF_FOLDER}/{filename}"
        )

        loader = PyPDFLoader(
            pdf_path
        )

        pages = loader.load()

        chunks = splitter.split_documents(
            pages
        )

        print(
            f"{filename}: "
            f"{len(chunks)} Chunks"
        )

        total_chunks += len(chunks)

    print(
        f"Gesamt Chunks: {total_chunks}"
    )


# ---------------------------------------------------
# Embeddings speichern
# ---------------------------------------------------

def store_embeddings():

    engine = create_engine(
        DATABASE_URL
    )

    embeddings = OllamaEmbeddings(
        model="nomic-embed-text",
        base_url=OLLAMA_URL
    )

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP
    )

    with engine.connect() as conn:

        result = conn.execute(
            text("""
                SELECT filename
                FROM documents
                WHERE status = 'processing'
            """)
        )

        files = [
            row[0]
            for row in result
        ]

    total_saved = 0

    for filename in files:

        try:

            print(
                f"Verarbeite: {filename}"
            )

            pdf_path = (
                f"{PDF_FOLDER}/{filename}"
            )

            loader = PyPDFLoader(
                pdf_path
            )

            pages = loader.load()

            chunks = splitter.split_documents(
                pages
            )

            if not chunks:
                raise ValueError(
                    f"Keine Text-Chunks in {filename} gefunden"
                )

            vectors = embeddings.embed_documents(
                [chunk.page_content for chunk in chunks]
            )

            if len(vectors) != len(chunks):
                raise ValueError(
                    f"Embedding-Anzahl stimmt für {filename} nicht mit der Chunk-Anzahl überein"
                )

            with engine.begin() as conn:
                conn.execute(
                    text("""
                        DELETE FROM pdf_embeddings
                        WHERE source_file = :filename
                    """),
                    {"filename": filename}
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
                            "source_file": filename,
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

            total_saved += len(chunks)

            print(
                f"{filename}: "
                f"{len(chunks)} Embeddings gespeichert"
            )

        except Exception as e:

            print(
                f"FEHLER bei {filename}: {e}"
            )

            with engine.begin() as conn:

                conn.execute(
                    text("""
                        UPDATE documents
                        SET status = 'failed'
                        WHERE filename = :filename
                    """),
                    {
                        "filename": filename
                    }
                )

    print(
        f"Gesamt gespeicherte Embeddings: "
        f"{total_saved}"
    )


# ---------------------------------------------------
# Status auf indexed setzen
# ---------------------------------------------------

def mark_indexed():

    engine = create_engine(
        DATABASE_URL
    )

    with engine.begin() as conn:

        conn.execute(
            text("""
                UPDATE documents
                SET status = 'indexed'
                WHERE status = 'processing'
            """)
        )

    print(
        "Dokumente erfolgreich indexiert"
    )


# ---------------------------------------------------
# DAG
# ---------------------------------------------------

# ---------------------------------------------------
# DAG
# ---------------------------------------------------

with DAG(
    dag_id="pdf_rag_pipeline",
    description="PDF RAG Pipeline",
    start_date=datetime(
        2025,
        1,
        1
    ),
    catchup=False,
    schedule="*/5 * * * *",
    tags=[
        "pdf",
        "rag",
        "ollama",
        "pgvector",
        "dataset"
    ],
) as dag:

    get_documents = PythonOperator(
        task_id="get_uploaded_documents",
        python_callable=get_uploaded_documents
    )

    processing = PythonOperator(
        task_id="mark_processing",
        python_callable=mark_processing
    )

    extract_task = PythonOperator(
        task_id="extract_text",
        python_callable=extract_text
    )

    chunk_task = PythonOperator(
        task_id="chunk_documents",
        python_callable=chunk_documents
    )

    store_task = PythonOperator(
        task_id="store_embeddings",
        python_callable=store_embeddings,

        # Dataset Event erzeugen
        outlets=[pdf_embeddings_dataset]
    )

    indexed_task = PythonOperator(
        task_id="mark_indexed",
        python_callable=mark_indexed
    )

    (
        get_documents
        >> processing
        >> extract_task
        >> chunk_task
        >> store_task
        >> indexed_task
    )