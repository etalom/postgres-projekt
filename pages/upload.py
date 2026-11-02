import streamlit as st
from pathlib import Path
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

UPLOAD_DIR = Path(
    "../Data/incoming_pdfs"
)

UPLOAD_DIR.mkdir(
    parents=True,
    exist_ok=True
)

st.title("📤 PDF Upload")

files = st.file_uploader(
    "PDF auswählen",
    type=["pdf"],
    accept_multiple_files=True
)

if files:

    for file in files:

        path = (
            UPLOAD_DIR /
            file.name
        )

        with open(path, "wb") as f:

            f.write(
                file.getbuffer()
            )

        with engine.begin() as conn:

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
                    "filename": file.name
                }
            )

        st.success(
            f"{file.name} gespeichert"
        )