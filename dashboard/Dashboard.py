import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from streamlit_autorefresh import st_autorefresh


# --------------------------------------------------
# 1. Datenbank
# --------------------------------------------------

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://admin:Admi123!@localhost:5433/meinedb",
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# --------------------------------------------------
# 2. Streamlit Config
# --------------------------------------------------

st.set_page_config(
    page_title="Echtzeit Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Echtzeit Dashboard")
st.caption("Monitoring für deinen RAG AI Assistant")


# --------------------------------------------------
# 3. Automatischer Refresh
# --------------------------------------------------

st_autorefresh(
    interval=5000,
    key="dashboard_refresh",
)


# --------------------------------------------------
# 4. Daten laden
# --------------------------------------------------

def load_data(sql):
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn)


try:

    # KPIs
    df_kpis = load_data("""
        SELECT
            (SELECT COUNT(DISTINCT source_file)
             FROM pdf_embeddings) AS pdf_count,

            (SELECT COUNT(*)
             FROM pdf_embeddings) AS chunk_count,

            (SELECT COUNT(*)
             FROM query_log) AS question_count
    """)

    pdf_count = int(df_kpis.iloc[0]["pdf_count"])
    chunk_count = int(df_kpis.iloc[0]["chunk_count"])
    question_count = int(df_kpis.iloc[0]["question_count"])

    # Antwortzeiten
    df_time = load_data("""
        SELECT created_at, total_time
        FROM query_log
        WHERE created_at IS NOT NULL
        ORDER BY created_at
    """)

    # Anfragen pro Stunde
    df_hour = load_data("""
        SELECT
            date_trunc('hour', created_at) AS stunde,
            COUNT(*) AS anfragen
        FROM query_log
        WHERE created_at IS NOT NULL
        GROUP BY stunde
        ORDER BY stunde
    """)

    # Modellnutzung
    df_model = load_data("""
        SELECT
            COALESCE(model_name, 'Unbekannt') AS model_name,
            COUNT(*) AS anzahl
        FROM query_log
        GROUP BY model_name
        ORDER BY anzahl DESC
    """)

    # Dokumente und Chunks
    df_docs = load_data("""
        SELECT
            source_file,
            COUNT(*) AS chunks
        FROM pdf_embeddings
        GROUP BY source_file
        ORDER BY chunks DESC
    """)

    # Letzte 20 Anfragen
    df_recent = load_data("""
        SELECT
            question,
            model_name,
            total_time,
            created_at
        FROM query_log
        ORDER BY created_at DESC NULLS LAST
        LIMIT 20
    """)

except SQLAlchemyError as e:
    st.error("Fehler beim Laden der Daten aus PostgreSQL.")
    st.code(str(e))
    st.stop()


# --------------------------------------------------
# 5. Status
# --------------------------------------------------

st.caption(
    "Letzter Abruf: "
    + datetime.now().astimezone().strftime(
        "%d.%m.%Y %H:%M:%S %Z"
    )
)

k1, k2, k3 = st.columns(3)

k1.metric("📚 PDFs", f"{pdf_count:,}")
k2.metric("🧩 Chunks", f"{chunk_count:,}")
k3.metric("💬 Fragen", f"{question_count:,}")

st.divider()


# --------------------------------------------------
# 6. Antwortzeiten und Anfragen pro Stunde
# --------------------------------------------------

left, right = st.columns(2)

with left:
    with st.container(border=True):
        st.subheader("⏱ Antwortzeiten")

        if not df_time.empty:
            fig_time = px.line(
                df_time,
                x="created_at",
                y="total_time",
                markers=True,
                template="plotly_dark",
                labels={
                    "created_at": "Zeitpunkt",
                    "total_time": "Antwortzeit (Sekunden)",
                },
            )

            st.plotly_chart(
                fig_time,
                use_container_width=True,
            )
        else:
            st.info("Noch keine Antwortzeiten vorhanden.")

with right:
    with st.container(border=True):
        st.subheader("💬 Anfragen pro Stunde")

        if not df_hour.empty:
            fig_hour = px.bar(
                df_hour,
                x="stunde",
                y="anfragen",
                template="plotly_dark",
                labels={
                    "stunde": "Stunde",
                    "anfragen": "Anfragen",
                },
            )

            st.plotly_chart(
                fig_hour,
                use_container_width=True,
            )
        else:
            st.info("Noch keine Anfragen vorhanden.")


# --------------------------------------------------
# 7. Dokumente und Modellnutzung
# --------------------------------------------------

left, right = st.columns(2)

with left:
    with st.container(border=True):
        st.subheader("📚 Dokumente")

        if not df_docs.empty:
            fig_docs = px.bar(
                df_docs,
                x="chunks",
                y="source_file",
                orientation="h",
                template="plotly_dark",
                labels={
                    "chunks": "Chunks",
                    "source_file": "Dokument",
                },
            )

            st.plotly_chart(
                fig_docs,
                use_container_width=True,
            )
        else:
            st.info("Keine Dokumente gefunden.")

with right:
    with st.container(border=True):
        st.subheader("🤖 Modellnutzung")

        if not df_model.empty:
            fig_model = px.pie(
                df_model,
                names="model_name",
                values="anzahl",
                hole=0.5,
                template="plotly_dark",
            )

            st.plotly_chart(
                fig_model,
                use_container_width=True,
            )
        else:
            st.info("Noch keine Modellaufrufe vorhanden.")


# --------------------------------------------------
# 8. Live Activity Feed
# --------------------------------------------------

with st.container(border=True):
    st.subheader("🔥 Live Activity Feed")

    if not df_recent.empty:
        st.dataframe(
            df_recent,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Warte auf die erste Frage.")

st.caption(
    "Automatischer Datenbankabruf alle 5 Sekunden."
)