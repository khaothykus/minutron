import re
from pdfminer.high_level import extract_text

# Padrões principais
RX_OCORR = re.compile(r"OCORR:\s*([A-Z]{2}\d{8})")
RX_STATUS = re.compile(r"\*{3}\s*(BOM|RUIM|DOA)\s*\*{3}", re.I)
RX_NUMNF = re.compile(r"\bNota\s*Fiscal\b.*?(\d{5,12})", re.I | re.S)
RX_CNPJ = re.compile(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b")
RX_CPF  = re.compile(r"\b(\d{3}\.?\d{3}\.?\d{3}-?\d{2})\b")
RX_CEP  = re.compile(r"\b(\d{5}-?\d{3})\b")
RX_UF   = re.compile(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b")
RX_IE   = re.compile(r"Inscri[çc][aã]o\s*Estadual[: ]+([A-Z0-9\.-/]+)", re.I)
RX_TRANSP = re.compile(r"Transportadora[: ]+(.+)", re.I)
RX_END = re.compile(r"Endere[çc]o[: ]+(.+)", re.I)

def is_danfe(path: str) -> bool:
    try:
        txt = extract_text(path) or ""
    except Exception:
        return False
    T = txt.upper()
    return "DANFE" in T or "NOTA FISCAL" in T or "NF-E" in T

def parse_status(txt: str) -> str:
    m = RX_STATUS.search(txt)
    return m.group(1).upper() if m else ""

def parse_header(txt: str) -> dict:
    nome = _grab_after(txt, r"Remetente[: ]+(.+)")
    cpf = _first(RX_CPF.findall(txt)) or ""
    cnpj = _first(RX_CNPJ.findall(txt)) or ""
    ie = _first(RX_IE.findall(txt)) or ""
    transp = _first(RX_TRANSP.findall(txt)) or ""
    end = _first(RX_END.findall(txt)) or ""
    rua, numero, bairro, cidade, uf, cep = _split_endereco(end, txt)
    nf = _first(RX_NUMNF.findall(txt)) or ""
    return {
        "numero_nf": nf,
        "nome_remetente": nome.strip(),
        "cpf_remetente": _clean(cpf),
        "rua_emitente": rua,
        "numero_emitente": numero,
        "bairro_emitente": bairro,
        "cidade_emitente": cidade,
        "uf_emitente": uf,
        "cep_emitente": _clean(cep),
        "cnpj_emitente": _clean(cnpj),
        "ie_emitente": ie.strip(),
        "transportador": transp.strip(),
    }

def parse_produtos(txt: str) -> list[dict]:
    produtos = []
    nf = _first(RX_NUMNF.findall(txt)) or ""
    status = parse_status(txt) or "BOM"  # fallback
    for m in RX_OCORR.finditer(txt):
        ocorr = m.group(1)  # já vem limpo no formato AA99999999
        codigo = _grab_near(txt, ocorr, r"C[ÓO]D\.?PROD\.?:?\s*([A-Z0-9\.\-\/]+)")
        qtde = _grab_near(txt, ocorr, r"QTDE\.?:?\s*([\d\.]+(?:,\d{1,3})?)")
        vtot = _grab_near(txt, ocorr, r"(?:V\.?TOTAL|VALOR\s+NF)[: ]*\s*([\d\.]+,\d{2})")
        produtos.append({
            "ocorrencia": ocorr,
            "status": status,
            "qtde": _to_num(qtde),
            "numero_nf": nf,
            "codigo_prod": (codigo or "").strip(),
            "valor_nf": _to_num(vtot),
        })
    return produtos

def parse_lote(pdf_paths: list[str]) -> tuple[dict, list[dict]]:
    header = None
    all_prod = []
    for i, p in enumerate(pdf_paths):
        txt = extract_text(p) or ""
        if i == 0:
            header = parse_header(txt)
        all_prod.extend(parse_produtos(txt))
    return header or {}, all_prod

# Utilitários
def _clean(s): return re.sub(r"[^\d]", "", s or "")
def _first(lst): return lst[0] if lst else ""

def _grab_after(txt: str, pattern: str) -> str:
    m = re.search(pattern, txt, re.I)
    return m.group(1).strip() if m else ""

def _grab_near(txt: str, anchor: str, pattern: str) -> str | None:
    pos = txt.find(anchor)
    if pos == -1: return None
    win = txt[pos:pos+300]
    m = re.search(pattern, win, re.I)
    return m.group(1) if m else None

def _split_endereco(line: str, txt: str):
    cep = _first(RX_CEP.findall(txt)) or ""
    uf = _first(RX_UF.findall(txt)) or ""
    rua, numero, bairro, cidade = "", "", "", ""
    m = re.search(r"(.+?),\s*(\d+)\s*-\s*(.+?)\s*-\s*(.+)", line)
    if m:
        rua, numero, bairro, cidade = m.groups()
    else:
        rua = line
    return rua.strip(), numero.strip(), bairro.strip(), cidade.strip(), uf, cep

def _to_num(s: str | None) -> float:
    if not s: return 0.0
    s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0