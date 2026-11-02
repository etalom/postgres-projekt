from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from pathlib import Path

import os
import shutil
import pandas as pd

from sqlalchemy import create_engine

# -----------------------------------
# Konfiguration
# -----------------------------------

DATABASE_URL = (
    "postgresql+psycopg2://"
    "admin:Admi123!@postgres:5432/meinedb"
)

DATA_FOLDER = (
    "/opt/airflow/data/raw"
)

ARCHIVE_FOLDER = (
    "/opt/airflow/data/archive"
)

# -----------------------------------
# CSV-Dateien finden
# -----------------------------------

def discover_csv_files():

    csv_files = []

    for file in Path(DATA_FOLDER).glob("*.csv"):

        csv_files.append(
            str(file)
        )

    print(
        f"Gefundene CSV-Dateien: {csv_files}"
    )

    return csv_files


# -----------------------------------
# CSVs nach PostgreSQL laden
# -----------------------------------

def load_csv_files(**context):

    files = context["ti"].xcom_pull(
        task_ids="discover_csv_files"
    )

    if not files:

        print(
            "Keine CSV-Dateien gefunden."
        )

        return

    engine = create_engine(
        DATABASE_URL
    )

    os.makedirs(
        ARCHIVE_FOLDER,
        exist_ok=True
    )

    for file_path in files:

        try:

            file_name = (
                Path(file_path)
                .stem
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )

            table_name = (
                f"raw_{file_name}"
            )

            print(
                f"\nLade Datei: {file_path}"
            )

            df = pd.read_csv(
                file_path,
                low_memory=False
            )

            print(
                f"Zeilen: {len(df)}"
            )

            df.to_sql(
                table_name,
                engine,
                if_exists="replace",
                index=False
            )

            print(
                f"✅ Tabelle {table_name} erstellt"
            )

            archive_path = (
                f"{ARCHIVE_FOLDER}/"
                f"{Path(file_path).name}"
            )

            shutil.move(
                file_path,
                archive_path
            )

            print(
                f"📦 Archiviert: {archive_path}"
            )

        except Exception as e:

            print(
                f"❌ Fehler bei "
                f"{file_path}: {e}"
            )


# -----------------------------------
# Übersicht anzeigen
# -----------------------------------

def show_tables():

    engine = create_engine(
        DATABASE_URL
    )

    query = """
        SELECT
            table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        ORDER BY table_name
    """

    df = pd.read_sql(
        query,
        engine
    )

    print(df)


# -----------------------------------
# DAG
# -----------------------------------

with DAG(
    dag_id="load_all_csv_files",
    description="Automatische CSV ELT Pipeline",
    start_date=datetime(
        2025,
        1,
        1
    ),
    catchup=False,
    schedule="*/10 * * * *",
    tags=[
        "elt",
        "csv",
        "postgres"
    ],
) as dag:

    discover_task = PythonOperator(
        task_id="discover_csv_files",
        python_callable=discover_csv_files
    )

    load_task = PythonOperator(
        task_id="load_csv_files",
        python_callable=load_csv_files
    )

    overview_task = PythonOperator(
        task_id="show_tables",
        python_callable=show_tables
    )

    (
        discover_task
        >> load_task
        >> overview_task
    )