import re
import pdfplumber
from services.danfe_utils import _to_num, _clean, _first
from services.danfe_regex import RX_OCORR, RX_NUMNF, RX_STATUS

def extrair_produtos_tabela(pdf_path: str, txt: str) -> list[dict]:
    produtos = []

    ocorrencia_match = RX_OCORR.search(txt)
    ocorrencia = ocorrencia_match.group(1) if ocorrencia_match else ""
    numero_nf = _first(RX_NUMNF.findall(txt))
    status = _first(RX_STATUS.findall(txt))

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                headers = table[0]
                if headers and "CÓD.PROD." in headers:
                    for row in table[1:]:
                        def limpa(campo):
                            return re.sub(r"[\n\-]+", "", campo or "").strip()

                        if limpa(row[0]) and limpa(row[1]):
                            produtos.append({
                                "codigo_prod": limpa(row[0]),
                                "descricao": limpa(row[1]),
                                "qtde": _to_num(limpa(row[6])),
                                "valor_unit": _to_num(limpa(row[7])),
                                "valor_nf": _to_num(limpa(row[8])),
                                "ocorrencia": ocorrencia,
                                "numero_nf": numero_nf,
                                "status": status,
                            })
    return produtos

def parse_produtos(txt: str) -> list[dict]:
    produtos = []
    nf = _first(RX_NUMNF.findall(txt)) or ""
    status = _first(RX_STATUS.findall(txt)) or "BOM"

    for m in re.finditer(r"ITEM:(\d{6})\s+OCORR:([A-Z]{2}\d{8})", txt):
        item, ocorr = m.groups()
        bloco = txt[m.start():m.start()+300]

        qtde = _grab_near(bloco, ocorr, r"QTDE[: ]*([\d\.]+(?:,\d{1,3})?)")
        vtot = _grab_near(bloco, ocorr, r"(?:VALOR\s+NF|V\.?TOTAL)[: ]*([\d\.]+,\d{2})")
        codigo = _grab_near(bloco, ocorr, r"C[ÓO]D\.?PROD\.?:?\s*([A-Z0-9\.\-\/]+)")

        produtos.append({
            "ocorrencia": ocorr,
            "status": status,
            "qtde": _to_num(qtde),
            "numero_nf": nf,
            "codigo_prod": (codigo or "").strip(),
            "valor_nf": _to_num(vtot),
        })

    return produtos

def _grab_near(txt: str, anchor: str, pattern: str) -> str | None:
    pos = txt.find(anchor)
    if pos == -1:
        return None
    win = txt[pos:pos+300]
    m = re.search(pattern, win, re.I)
    return m.group(1) if m else None
