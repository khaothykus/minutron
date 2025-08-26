import os, math, shutil, tempfile
from datetime import datetime
from spire.xls import Workbook, FileFormat
from services.pdf_tools import merge_pdfs
from config import TEMPLATE_PATH

COLS = ["Ocorrência", "RAT", "Qtde", "Nota Fiscal", "Código", "Valor NF"]
ROWS_START, ROWS_END = 9, 38
ROWS_PER_PAGE = ROWS_END - ROWS_START + 1

MESES = ["","Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

def _replace_tokens(ws, tokens: dict):
    for k, v in tokens.items():
        ws.Replace(k, v or "")

def _find_header_cols(ws) -> dict:
    for r in range(1, 25):
        names = {}
        for c in range(1, 25):
            txt = ws.Range[r, c].Text
            if txt:
                names[txt.strip()] = c
        if all(x in names for x in COLS):
            return {x: names[x] for x in COLS}
    raise RuntimeError("Cabeçalho da tabela não encontrado. Verifique o template e os títulos das colunas.")

def _fill_table(ws, cols_map: dict, produtos_slice: list[dict]):
    r = ROWS_START
    for item in produtos_slice:
        ws.Range[r, cols_map["Ocorrência"]].Text = item["ocorrencia"]
        ws.Range[r, cols_map["RAT"]].Text = item.get("rat", "") or ""
        ws.Range[r, cols_map["Qtde"]].NumberValue = float(item["qtde"])
        ws.Range[r, cols_map["Nota Fiscal"]].Text = str(item["numero_nf"])
        ws.Range[r, cols_map["Código"]].Text = item["codigo_prod"]
        ws.Range[r, cols_map["Valor NF"]].NumberValue = float(item["valor_nf"])
        r += 1

def preencher_e_exportar_lote(qlid: str, cidade: str, header: dict, produtos: list[dict], data_iso: str, volumes: int, out_pdf_path: str):
    produtos = sorted(produtos, key=lambda x: int("0" + "".join(filter(str.isdigit, str(x["numero_nf"])))))
    dt = datetime.fromisoformat(data_iso)

    tokens = {
        "{{LOCAL}}": cidade,
        "{{DIA}}": f"{dt.day:02d}",
        "{{MES}}": MESES[dt.month],
        "{{ANO}}": str(dt.year),
        "{{DATA}}": dt.strftime("%d/%m/%Y"),
        "{{VOLUMES}}": str(max(1, int(volumes))),
        "{{NOME_REMETENTE}}": header.get("nome_remetente",""),
        "{{CPF_REMETENTE}}": header.get("cpf_remetente",""),
        "{{RUA_EMITENTE}}": header.get("rua_emitente",""),
        "{{NUMERO_EMITENTE}}": header.get("numero_emitente",""),
        "{{BAIRRO_EMITENTE}}": header.get("bairro_emitente",""),
        "{{CIDADE_EMITENTE}}": header.get("cidade_emitente",""),
        "{{UF_EMITENTE}}": header.get("uf_emitente",""),
        "{{CEP_EMITENTE}}": header.get("cep_emitente",""),
        "{{CNPJ_EMITENTE}}": header.get("cnpj_emitente",""),
        "{{IE_EMITENTE}}": header.get("ie_emitente",""),
        "{{TRANSPORTADOR}}": header.get("transportador",""),
    }

    pages = max(1, math.ceil(len(produtos) / ROWS_PER_PAGE))
    tmpdir = tempfile.mkdtemp(prefix="minuta_")
    pdfs = []

    try:
        for i in range(pages):
            wb = Workbook()
            wb.LoadFromFile(TEMPLATE_PATH)
            ws = wb.Worksheets[0]

            _replace_tokens(ws, tokens)
            cols = _find_header_cols(ws)

            slice_i = produtos[i*ROWS_PER_PAGE:(i+1)*ROWS_PER_PAGE]
            _fill_table(ws, cols, slice_i)

            page_pdf = os.path.join(tmpdir, f"page_{i+1}.pdf")
            wb.SaveToFile(page_pdf, FileFormat.PDF)
            pdfs.append(page_pdf)

        merge_pdfs(pdfs, out_pdf_path)
    finally:
        # limpeza de temporários acontece automática quando o container recicla;
        # se quiser, adicione remoção manual aqui.
        pass