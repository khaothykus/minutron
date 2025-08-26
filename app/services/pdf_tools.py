from PyPDF2 import PdfMerger

def merge_pdfs(pdf_paths, out_path):
    merger = PdfMerger()
    for p in pdf_paths:
        merger.append(p)
    with open(out_path, "wb") as f:
        merger.write(f)
    merger.close()