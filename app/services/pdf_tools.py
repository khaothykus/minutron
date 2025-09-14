# app/services/pdf_tools.py
import io
from PIL import Image
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas

def merge_pdfs(inputs, output_path):
    writer = PdfWriter()
    for src in inputs:
        reader = PdfReader(src)
        for page in reader.pages:
            writer.add_page(page)
    with open(output_path, "wb") as f:
        writer.write(f)

def _overlay_pdf_bytes(page_w, page_h, logo_path, x_pt, y_pt, w_pt):
    with Image.open(logo_path) as im:
        iw, ih = im.size
    h_pt = w_pt * ih / iw
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(page_w, page_h))
    c.drawImage(logo_path, x_pt, y_pt, width=w_pt, height=h_pt, mask="auto")
    c.save()
    buf.seek(0)
    return PdfReader(buf)

# def overlay_logo_on_pdf(pdf_in, pdf_out, logo_path, x_mm, top_mm, width_mm):
#     mm = 72.0 / 25.4
#     reader = PdfReader(pdf_in)
#     writer = PdfWriter()
#     w = float(reader.pages[0].mediabox.width)
#     h = float(reader.pages[0].mediabox.height)

#     w_pt = width_mm * mm
#     x_pt = x_mm * mm
#     with Image.open(logo_path) as im:
#         iw, ih = im.size
#     h_pt = w_pt * ih / iw
#     y_pt = h - top_mm * mm - h_pt

#     ov = _overlay_pdf_bytes(w, h, logo_path, x_pt, y_pt, w_pt).pages[0]
#     for page in reader.pages:
#         page.merge_page(ov)
#         writer.add_page(page)
#     with open(pdf_out, "wb") as f:
#         writer.write(f)

def overlay_logo_on_pdf(pdf_in, pdf_out, logo_path, x_mm, top_mm, width_mm, align="left", margin_mm=15):
    mm = 72.0 / 25.4
    r = PdfReader(pdf_in)
    w = float(r.pages[0].mediabox.width)
    h = float(r.pages[0].mediabox.height)

    w_pt = width_mm * mm
    # posição horizontal
    if (align or "").lower() == "center":
        x_pt = (w - w_pt) / 2.0
    elif (align or "").lower() == "right":
        x_pt = w - (margin_mm * mm) - w_pt
    else:  # left
        x_pt = x_mm * mm

    # posição vertical (a partir do topo)
    with Image.open(logo_path) as im: iw, ih = im.size
    h_pt = w_pt * ih / iw
    y_pt = h - top_mm * mm - h_pt

    ov = _overlay_pdf_bytes(w, h, logo_path, x_pt, y_pt, w_pt).pages[0]
    W = PdfWriter()
    for p in r.pages:
        p.merge_page(ov)
        W.add_page(p)
    with open(pdf_out, "wb") as f: W.write(f)
