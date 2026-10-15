from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "postgresql://admin:passwort@localhost:5433/meinedb"
)

df = pd.read_csv("data/winequality.csv")

df.to_sql(
    "winequality",
    engine,
    if_exists="replace",
    index=False
)

print("Import erfolgreich")