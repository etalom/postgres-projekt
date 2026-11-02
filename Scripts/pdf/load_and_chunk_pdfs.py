from pathlib import Path

from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_ollama import OllamaEmbeddings

PDF_DIR = Path("../Data/pdfs")

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1500,
    chunk_overlap=300,
    separators=[
        "\n\n",
        "\n",
        ". ",
        " ",
        ""
    ]
)

all_chunks = []

pdf_files = list(PDF_DIR.glob("*.pdf"))

print(f"{len(pdf_files)} PDFs gefunden\n")

for pdf_file in pdf_files:

    print(f"Verarbeite: {pdf_file.name}")

    loader = PyPDFLoader(str(pdf_file))

    pages = loader.load()

    chunks = splitter.split_documents(pages)

    print(
        f"  Seiten: {len(pages)}"
    )

    print(
        f"  Chunks: {len(chunks)}"
    )

    for index, chunk in enumerate(chunks):

        chunk.metadata["source_file"] = (
            pdf_file.name
        )

        chunk.metadata["chunk_number"] = (
            index
        )

    all_chunks.extend(chunks)

print(
    f"\nGesamtanzahl Chunks: {len(all_chunks)}"
)

if all_chunks:

    print("\nBeispiel Chunk:\n")

    print(
        all_chunks[0]
        .page_content[:1000]
    )

    print("\nMetadaten:\n")

    print(
        all_chunks[0]
        .metadata
    )

    print("\nEmbedding wird erzeugt...\n")

    embeddings = OllamaEmbeddings(
        model="nomic-embed-text"
    )

    vector = embeddings.embed_query(
        all_chunks[0].page_content
    )

    print(
        f"Vektorgröße: {len(vector)}"
    )

    print(
        "\nErste 10 Werte des Vektors:"
    )

    print(
        vector[:10]
    )