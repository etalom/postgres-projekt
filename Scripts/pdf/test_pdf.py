from langchain_community.document_loaders import PyPDFLoader

loader = PyPDFLoader(
    "../Data/pdfs/GWB.pdf"
)

pages = loader.load()

print("Seiten:", len(pages))

print(pages[0].page_content[:500])