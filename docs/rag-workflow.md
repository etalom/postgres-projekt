# RAG-Workflow und Datenmodell

Dieses Modell beschreibt die im Repository erkennbare PDF-RAG-Implementierung.
Die Diagramme dokumentieren den Ist-Zustand; sie ersetzen kein Datenbank-DDL.

## PDF-Indexierung

```mermaid
flowchart TD
    U[Benutzer] --> UP[Streamlit: pages/upload.py]
    UP -->|PDF speichern| IN[(Data/incoming_pdfs)]
    UP -->|documents: status uploaded| DB[(PostgreSQL + pgvector)]

    subgraph AF[Airflow DAG: pdf_rag_pipeline, alle 5 Minuten]
        S[get_uploaded_documents<br/>Dateien finden und registrieren]
        P[mark_processing]
        E[extract_text<br/>Seiten lesen]
        C[chunk_documents<br/>Text in Chunks teilen]
        V[store_embeddings<br/>PDF erneut lesen und Chunks bilden]
        I[mark_indexed]
        S --> P --> E --> C --> V --> I
    end

    IN --> S
    DB --> S
    P --> DB
    E --> IN
    C --> IN
    V --> IN
    V -->|nomic-embed-text| O[Ollama]
    V -->|Chunks und Vektoren speichern| DB
    V -. bei Fehler .-> F[documents.status = failed]
    F --> DB
    V -. Airflow Dataset-Ereignis .-> DS[pdf_embeddings]
```

`chunk_documents` protokolliert die Chunk-Anzahl, speichert die Chunks aber
nicht. `store_embeddings` liest und chunked die PDFs deshalb erneut; dort
werden die Embeddings tatsächlich in `pdf_embeddings` geschrieben. Die
Chunk-Größe ist 1000 Zeichen mit 300 Zeichen Überlappung.

## Frage und Antwort

```mermaid
flowchart LR
    Q[Frage] --> EQ[Frage als Vektor<br/>nomic-embed-text]
    EQ -->|Cosinus-Distanz, Top 5| DB[(pdf_embeddings<br/>PostgreSQL + pgvector)]
    DB --> K[Kontext aus Treffern]
    Q --> L[Prompt mit Frage und Kontext]
    K --> L
    L -->|llama3.2| O[Ollama]
    O --> A[Antwort]
```

Dieser Suchpfad ist in `Scripts/pdf/search_test.py` umgesetzt. Er ist im
Repository nicht als Airflow-Task modelliert.

## Datenmodell

```mermaid
erDiagram
    DOCUMENTS ||--o{ PDF_EMBEDDINGS : "filename = source_file (logische Beziehung)"
    DOCUMENTS {
        string filename PK "eindeutig angenommen: ON CONFLICT (filename)"
        string status "uploaded, processing, indexed oder failed"
        timestamp uploaded_at
    }
    PDF_EMBEDDINGS {
        string source_file "Dateiname des Quelldokuments"
        integer chunk_number
        integer page_number
        text chunk_text
        vector embedding
    }
    QUERY_LOG {
        text question
        string model_name
        number total_time
        timestamp created_at
    }
```

`DOCUMENTS` und `PDF_EMBEDDINGS` sind aus den Upload-, DAG- und Suchabfragen
abgeleitet. `filename` wird als eindeutig behandelt, weil der Code
`ON CONFLICT (filename)` verwendet. Die Beziehung zwischen `filename` und
`source_file` ist im Anwendungscode nachvollziehbar; ein Datenbank-Fremdschlüssel
oder Primärschlüssel für `PDF_EMBEDDINGS` ist im Repository nicht definiert.

`QUERY_LOG` ist separat aufgeführt, weil das Dashboard diese Spalten abfragt.
Im Repository ist jedoch weder ein DDL für diese Tabelle noch ein Schreibpfad
für die Anfragen enthalten. Ihre Existenz und Befüllung sind daher nicht durch
den Code belegt.
