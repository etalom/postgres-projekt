import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb"
)

st.title("📚 Dokumente")

df = pd.read_sql(
    """
    SELECT *
    FROM documents
    ORDER BY uploaded_at DESC
    """,
    engine
)

st.dataframe(
    df,
    use_container_width=True
)