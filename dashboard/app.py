import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# --------------------------------------------------
# Seitenkonfiguration
# --------------------------------------------------

st.set_page_config(
    page_title="Wine Quality Dashboard",
    page_icon="🍷",
    layout="wide"
)

# --------------------------------------------------
# Design / CSS
# --------------------------------------------------

st.markdown("""
<style>

.stApp {
    background: linear-gradient(
        135deg,
        #fff5f5 0%,
        #fde2e4 50%,
        #f8d7da 100%
    );
}

div[data-testid="metric-container"] {
    background-color: white;
    border-radius: 15px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    border-left: 5px solid #7b1e3f;
}

h1, h2, h3 {
    color: #7b1e3f;
}

</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Header
# --------------------------------------------------

st.markdown("""
<div style="
background: linear-gradient(90deg,#7b1e3f,#a93f55);
padding:25px;
border-radius:20px;
text-align:center;
color:white;
margin-bottom:20px;
">
<h1 style="color:white;">
🍷 Wine Quality Dashboard
</h1>
<p>
Mein Projekt.
</p>
</div>
""", unsafe_allow_html=True)

# --------------------------------------------------
# Datenbankverbindung
# --------------------------------------------------

engine = create_engine(
    "postgresql+psycopg://admin:POSTGRES_PASSWORD@localhost:5433/meinedb"
)

# Tabelle laden
df = pd.read_sql(
    "SELECT * FROM winequalityN",
    engine
)

# --------------------------------------------------
# KPIs
# --------------------------------------------------

col1, col2, col3 = st.columns(3)

col1.metric(
    "📊 Datensätze",
    f"{len(df):,}"
)

col2.metric(
    "🍷 Ø Alkohol",
    round(df["alcohol"].mean(), 2)
)

col3.metric(
    "⭐ Ø Qualität",
    round(df["quality"].mean(), 2)
)

st.markdown("---")

# --------------------------------------------------
# Diagramm Alkoholgehalt
# --------------------------------------------------

st.subheader("🍷 Durchschnittlicher Alkoholgehalt")

avg_alcohol = (
    df.groupby("type")["alcohol"]
    .mean()
    .reset_index()
)

fig1 = px.bar(
    avg_alcohol,
    x="type",
    y="alcohol",
    color="type",
    title="Durchschnittlicher Alkoholgehalt nach Weintyp",
    color_discrete_sequence=["#7b1e3f", "#d63384"]
)

st.plotly_chart(fig1, use_container_width=True)

# --------------------------------------------------
# Qualitätsverteilung
# --------------------------------------------------

st.subheader("⭐ Qualitätsverteilung")

fig2 = px.histogram(
    df,
    x="quality",
    nbins=10,
    color_discrete_sequence=["#7b1e3f"]
)

st.plotly_chart(fig2, use_container_width=True)

# --------------------------------------------------
# Datenvorschau
# --------------------------------------------------

st.subheader("📄 Datenvorschau")

st.dataframe(
    df.head(20),
    use_container_width=True
)

# --------------------------------------------------
# Statistische Übersicht
# --------------------------------------------------

st.subheader("📈 Statistische Übersicht")

st.dataframe(
    df.describe(),
    use_container_width=True
)