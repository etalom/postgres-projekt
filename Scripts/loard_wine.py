import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg://admin:Admi123!@postgres:5432/meinedb"
)

df = pd.read_csv(
    "/opt/airflow/data/winequality.csv"
)

df.to_sql(
    "winequality",
    engine,
    if_exists="replace",
    index=False
)

print(f"{len(df)} Datensätze importiert")