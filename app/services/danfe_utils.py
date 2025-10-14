import re
from pdfminer.high_level import extract_text

def _clean(s: str | None) -> str:
    return re.sub(r"[^\d]", "", s or "")

def _first(lst: list) -> str:
    return lst[0] if lst else ""

def _to_num(s: str | None) -> float:
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def _grab_after(txt: str, pattern: str) -> str:
    m = re.search(pattern, txt, re.I)
    return m.group(1).strip() if m else ""

def _grab_near(txt: str, anchor: str, pattern: str) -> str | None:
    pos = txt.find(anchor)
    if pos == -1:
        return None
    win = txt[pos:pos+300]
    m = re.search(pattern, win, re.I)
    return m.group(1) if m else None

def formatar_ie(ie: str) -> str:
    ie = _clean(ie)
    if len(ie) == 12:
        return f"{ie[:3]}.{ie[3:6]}.{ie[6:9]}.{ie[9:]}"
    return ie

# def is_danfe(path: str) -> bool:
#     try:
#         txt = extract_text(path) or ""
#     except Exception:
#         return False
#     T = txt.upper()
#     return "DANFE" in T or "NOTA FISCAL" in T or "NF-E" in T


def is_danfe(path: str) -> bool:
    """
    Validação estrita de DANFE (NF-e impressa) da empresa:
      - Título: "DANFE" OU "Documento Auxiliar da Nota Fiscal Eletrônica"
      - Chave:  "Chave de Acesso" OU um bloco de 44 dígitos
      - Emitente: "NCR BRASIL LTDA" (aceita com ou sem ponto final)
    Rejeita arquivos gerados pelo próprio bot (minuta/com_danfes).
    """
    try:
        if not path or not path.lower().endswith(".pdf"):
            return False

        # extrai texto (1–2 primeiras páginas para desempenho); usa pypdfium2 e cai p/ pdfminer
        txt = ""
        try:
            import pypdfium2 as pdfium
            pdf = pdfium.PdfDocument(path)
            pages = min(len(pdf), 2)
            for i in range(pages):
                page = pdf.get_page(i)
                tp = page.get_textpage()
                txt += tp.get_text_bounded() or ""
        except Exception:
            try:
                # from pdfminer.high_level import extract_text
                # pdfminer lê tudo; ok em último caso
                txt = extract_text(path) or ""
            except Exception:
                return False

        txt = txt.upper()
        if not txt:
            return False

        has_title = ("DANFE" in txt) or ("DOCUMENTO AUXILIAR DA NOTA FISCAL ELETRÔNICA" in txt)
        has_key   = ("CHAVE DE ACESSO" in txt) or (re.search(r"\b\d{44}\b", txt) is not None)

        # Emitente: aceita variações "NCR BRASIL LTDA" e "NCR BRASIL LTDA."
        # Usa regex com \s+ e ponto opcional:
        company_ok = re.search(r"\bNCR\s+BRASIL\s+LTDA\.?\b", txt) is not None

        return has_title and has_key and company_ok
    except Exception:
        return False


def formatar_valor(valor: float) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
