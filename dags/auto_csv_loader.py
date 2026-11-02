from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

from datetime import datetime

import os
import shutil
import pandas as pd

from sqlalchemy import create_engine, text

# ---------------------------------------------------
# Konfiguration
# ---------------------------------------------------

INCOMING_FOLDER = "/opt/airflow/data/incoming"

ARCHIVE_FOLDER = "/opt/airflow/data/archive"

DATABASE_URL = (
    "postgresql+psycopg2://"
    "admin:Admi123!@postgres:5432/meinedb"
)

# ---------------------------------------------------
# Dataset Definition
# ---------------------------------------------------

raw_dataset = Dataset(
    "postgres://meinedb/raw_csv_loaded"
)

# ---------------------------------------------------
# CSV-Dateien finden
# ---------------------------------------------------

def extract_files():

    print(
        f"INCOMING_FOLDER: {INCOMING_FOLDER}"
    )

    folder_content = os.listdir(
        INCOMING_FOLDER
    )

    print(
        f"Ordnerinhalt: {folder_content}"
    )

    files = [
        f
        for f in folder_content
        if f.lower().endswith(".csv")
    ]

    print(
        f"CSV-Dateien: {files}"
    )

    if not files:

        print(
            "Keine CSV-Dateien gefunden."
        )

        return []

    return files


# ---------------------------------------------------
# Bereits importierte Dateien prüfen
# ---------------------------------------------------

def check_duplicates(**context):

    files = context["ti"].xcom_pull(
        task_ids="extract_files"
    )

    if not files:

        print(
            "Keine Dateien zum Prüfen."
        )

        return []

    engine = create_engine(
        DATABASE_URL
    )

    new_files = []

    with engine.connect() as conn:

        for file in files:

            result = conn.execute(
                text("""
                    SELECT COUNT(*)
                    FROM file_import_log
                    WHERE filename = :filename
                """),
                {
                    "filename": file
                }
            )

            count = result.scalar()

            if count == 0:

                print(
                    f"Neue Datei: {file}"
                )

                new_files.append(file)

            else:

                print(
                    f"Bereits importiert: {file}"
                )

    if not new_files:

        print(
            "Keine neuen Dateien gefunden."
        )

        return []

    return new_files


# ---------------------------------------------------
# CSV validieren
# ---------------------------------------------------

def validate_files(**context):

    files = context["ti"].xcom_pull(
        task_ids="check_duplicates"
    )

    if not files:

        print(
            "Keine Dateien zum Validieren."
        )

        return []

    valid_files = []

    for file in files:

        filepath = os.path.join(
            INCOMING_FOLDER,
            file
        )

        try:

            df = pd.read_csv(
                filepath
            )

            if len(df) > 0:

                print(
                    f"{file} validiert "
                    f"({len(df)} Zeilen)"
                )

                valid_files.append(
                    file
                )

        except Exception as e:

            print(
                f"Fehler in {file}: {e}"
            )

    if not valid_files:

        print(
            "Keine gültigen CSV-Dateien gefunden."
        )

        return []

    return valid_files


# ---------------------------------------------------
# Daten transformieren
# ---------------------------------------------------

def transform_files(**context):

    files = context["ti"].xcom_pull(
        task_ids="validate_files"
    )

    if not files:

        print(
            "Keine Dateien zum Transformieren."
        )

        return []

    transformed_files = []

    for file in files:

        filepath = os.path.join(
            INCOMING_FOLDER,
            file
        )

        df = pd.read_csv(
            filepath
        )

        df.columns = (
            df.columns
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        before = len(df)

        df = df.drop_duplicates()

        after = len(df)

        print(
            f"{file}: "
            f"{before - after} "
            f"Duplikate entfernt"
        )

        df.to_csv(
            filepath,
            index=False
        )

        transformed_files.append(
            file
        )

    return transformed_files

# ---------------------------------------------------
# In PostgreSQL laden
# ---------------------------------------------------

def load_to_postgres(**context):

    files = context["ti"].xcom_pull(
        task_ids="transform_files"
    )

    if not files:

        print(
            "Keine Dateien zum Laden."
        )

        return []

    engine = create_engine(
        DATABASE_URL
    )

    for file in files:

        filepath = os.path.join(
            INCOMING_FOLDER,
            file
        )

        table_name = (
            os.path.splitext(file)[0]
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

        df = pd.read_csv(
            filepath
        )

        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False
        )

        print(
            f"{len(df)} Zeilen "
            f"in {table_name} geladen"
        )

    return files


# ---------------------------------------------------
# Import protokollieren
# ---------------------------------------------------

def register_import(**context):

    files = context["ti"].xcom_pull(
        task_ids="load_to_postgres"
    )

    if not files:

        print(
            "Keine Dateien zum Registrieren."
        )

        return []

    engine = create_engine(
        DATABASE_URL
    )

    with engine.begin() as conn:

        for file in files:

            conn.execute(
                text("""
                    INSERT INTO file_import_log
                    (filename)
                    VALUES (:filename)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "filename": file
                }
            )

            print(
                f"Import registriert: {file}"
            )

    return files

# ---------------------------------------------------
# Dateien archivieren
# ---------------------------------------------------

def archive_files(**context):

    files = context["ti"].xcom_pull(
        task_ids="register_import"
    )

    if not files:

        print(
            "Keine Dateien zum Archivieren."
        )

        return

    os.makedirs(
        ARCHIVE_FOLDER,
        exist_ok=True
    )

    for file in files:

        source = os.path.join(
            INCOMING_FOLDER,
            file
        )

        destination = os.path.join(
            ARCHIVE_FOLDER,
            file
        )

        shutil.move(
            source,
            destination
        )

        print(
            f"{file} archiviert"
        )

# ---------------------------------------------------
# DAG
# ---------------------------------------------------

with DAG(
    dag_id="auto_csv_loader",
    description="Automatische CSV ELT Pipeline",
    start_date=datetime(
        2025,
        1,
        1
    ),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=[
        "etl",
        "csv",
        "postgres",
        "dataset"
    ],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_files",
        python_callable=extract_files
    )

    duplicate_task = PythonOperator(
        task_id="check_duplicates",
        python_callable=check_duplicates
    )

    validate_task = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files
    )

    transform_task = PythonOperator(
        task_id="transform_files",
        python_callable=transform_files
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,

        # Dataset Event erzeugen
        outlets=[raw_dataset]
    )

    register_task = PythonOperator(
        task_id="register_import",
        python_callable=register_import
    )

    archive_task = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files
    )

    (
        extract_task
        >> duplicate_task
        >> validate_task
        >> transform_task
        >> load_task
        >> register_task
        >> archive_task
    )