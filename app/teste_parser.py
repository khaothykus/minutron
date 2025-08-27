from pdfminer.high_level import extract_text
import pdfplumber
from services.danfe_parser import extrair_danfe_completa

pdf_path = "app/data/users/RP250439/temp/4cccb8c0/pdfs/35250833033440001176550010011077401767984857.PDF"

with pdfplumber.open(pdf_path) as pdf:
    texto = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
    tabelas = [table for page in pdf.pages for table in page.extract_tables()]

emitente, produtos = extrair_danfe_completa(pdf_path, texto, tabelas)
print("🔹 Emitente:")
for k, v in emitente.items():
    print(f"{k}: {v}")

print("\n🔹 Produtos:")
for prod in produtos:
    print(prod)


