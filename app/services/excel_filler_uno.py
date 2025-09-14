# app/services/excel_filler_uno.py
# ---------------------------------------------------------
# Gera PDFs a partir de XLSX/ODS via LibreOffice UNO (headless).
# - Ajuste de página (A4, margens, escala 1x0 por padrão, centralização)
# - Área de impressão limitada ao conteúdo
# - Overlay opcional de logo no PDF (pypdf + reportlab)
# - Imports organizados p/ evitar interferência do loader do UNO
# ---------------------------------------------------------

# 1) IMPORTAR pdf_tools ANTES DO UNO (evita uno._uno_import interferir)
from services.pdf_tools import merge_pdfs, overlay_logo_on_pdf

import os, math, tempfile, time, subprocess
from datetime import datetime

# UNO e tipos
import uno
from com.sun.star.beans import PropertyValue
from com.sun.star.connection import NoConnectException
from com.sun.star.table import CellRangeAddress

# Projeto
from config import TEMPLATE_PATH
from services.danfe_utils import formatar_valor

# -------------------- Configs de layout --------------------

COLS = ["Ocorrência", "RAT", "Qtde", "Nota Fiscal", "Código", "Valor NF"]
ROWS_START, ROWS_END = 9, 38
ROWS_PER_PAGE = ROWS_END - ROWS_START + 1

MESES = ["","Janeiro","Fevereiro","Março","Abril","Maio","Junho",
         "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

# -------------------- Helpers UNO --------------------

def _uno_url(path: str) -> str:
    return uno.systemPathToFileUrl(os.path.abspath(path))

def _connect_lo(timeout_s: int = 15, port: int | None = None):
    """Conecta ao listener (sobe se não existir). Retorna o Desktop."""
    port = port or int(os.getenv("UNO_PORT", "2002"))
    ctx = uno.getComponentContext()
    smgr = ctx.ServiceManager
    resolver = smgr.createInstanceWithContext("com.sun.star.bridge.UnoUrlResolver", ctx)

    url = f"uno:socket,host=127.0.0.1,port={port};urp;StarOffice.ComponentContext"

    def _try_resolve():
        return resolver.resolve(url)

    try:
        ctx2 = _try_resolve()
    except NoConnectException:
        # Sobe o soffice headless listener
        soffice = [
            "soffice",
            "--headless", "--nologo", "--nodefault", "--nofirststartwizard",
            "--norestore", "--nolockcheck",
            f'--accept=socket,host=127.0.0.1,port={port};urp;StarOffice.ServiceManager',
        ]
        subprocess.Popen(soffice, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Espera ficar disponível
        t0 = time.time()
        while True:
            try:
                ctx2 = _try_resolve()
                break
            except NoConnectException:
                if time.time() - t0 > timeout_s:
                    raise
                time.sleep(0.3)

    smgr2 = ctx2.ServiceManager
    desktop = smgr2.createInstanceWithContext("com.sun.star.frame.Desktop", ctx2)
    return desktop

def _open_template(path: str):
    """Abre o template como planilha oculta (Calc)."""
    desktop = _connect_lo()
    args = []
    for k, v in [
        ("Hidden", True),
        ("ReadOnly", False),
        ("AsTemplate", False),
    ]:
        p = PropertyValue(); p.Name, p.Value = k, v; args.append(p)
    doc = desktop.loadComponentFromURL(_uno_url(path), "_blank", 0, tuple(args))
    return doc

def _export_pdf(doc, out_path: str):
    """Exporta a planilha corrente para PDF."""
    args = []
    p = PropertyValue(); p.Name = "FilterName"; p.Value = "calc_pdf_Export"; args.append(p)
    p = PropertyValue(); p.Name = "Overwrite"; p.Value = True; args.append(p)
    doc.storeToURL(_uno_url(out_path), tuple(args))

# -------------------- Ajuste de página / área de impressão --------------------

def _mm_to_100th(mm_str, default="10"):
    try:
        return int(float(mm_str) * 100)
    except Exception:
        return int(float(default) * 100)

def _used_bounds(sheet):
    """Pega exatamente o retângulo usado (A1 até última célula com conteúdo)."""
    cur = sheet.createCursor()
    try:
        cur.gotoStartOfUsedArea(False)
    except Exception:
        pass
    try:
        cur.gotoEndOfUsedArea(True)
    except Exception:
        pass
    addr = cur.RangeAddress
    # dá um pequeno respiro
    sr, er = max(0, addr.StartRow), min(10000, addr.EndRow + 1)
    sc, ec = max(0, addr.StartColumn), min(10000, addr.EndColumn + 1)
    return sr, er, sc, ec

def _set_print_area(sheet, sr, er, sc, ec):
    cra = CellRangeAddress()
    cra.Sheet = 0
    cra.StartRow, cra.EndRow = int(sr), int(er)
    cra.StartColumn, cra.EndColumn = int(sc), int(ec)
    sheet.PrintAreas = (cra,)

def _expand_rows_to_fill_height(sheet, sr, er, target_h_mm, content_h_mm, safety=0.98):
    """
    Aumenta proporcionalmente a altura das linhas [sr..er] para ocupar quase toda a altura disponível.
    safety < 1 deixa uma folguinha pra não quebrar em 2 páginas.
    """
    if content_h_mm <= 0:
        return
    factor = (target_h_mm / content_h_mm) * float(safety)
    if factor <= 1.02:  # se a diferença for pequena, não mexe
        return
    rows = sheet.Rows
    for r in range(sr, er + 1):
        h = rows.getByIndex(r).Height  # em 1/100 mm
        rows.getByIndex(r).Height = int(h * factor)

def _tunar_pagina(doc, title_rows="$1:$3", bounds=None):
    """A4 retrato, margens, escala (fit ou PageScale), área=usada por padrão.
       Respeita larguras/alturas do XLSX (sem OptimalWidth/Height)."""
    import os

    sheet = doc.Sheets.getByIndex(0)
    page_styles = doc.StyleFamilies.getByName("PageStyles")
    ps = page_styles.getByName(sheet.PageStyle)

    # 1) Formato/orientação (+ IsLandscape)
    try:
        from com.sun.star.view import PaperFormat, PaperOrientation
        fmt = os.getenv("PAGE_FORMAT", "A4").upper()
        pf = {"A4": PaperFormat.A4, "A3": PaperFormat.A3, "LETTER": PaperFormat.LETTER}.get(fmt, PaperFormat.A4)
        ps.PaperFormat = pf
        ori = os.getenv("PAGE_ORIENTATION", "PORTRAIT").upper()
        ps.PaperOrientation = getattr(PaperOrientation, ori, PaperOrientation.PORTRAIT)
        try:
            ps.IsLandscape = (ori == "LANDSCAPE")
        except Exception:
            pass
    except Exception:
        pass

    # 2) Margens (1/100 mm)
    for name, env, default in [
        ("TopMargin",    "MARGIN_TOP_MM",    "8"),
        ("BottomMargin", "MARGIN_BOTTOM_MM", "8"),
        ("LeftMargin",   "MARGIN_LEFT_MM",   "8"),
        ("RightMargin",  "MARGIN_RIGHT_MM",  "8"),
    ]:
        try:
            setattr(ps, name, _mm_to_100th(os.getenv(env, default)))
        except Exception:
            pass

    # 3) Área de impressão = usada (ou a passada por parâmetro)
    try:
        sr, er, sc, ec = bounds if bounds else _used_bounds(sheet)
        _set_print_area(sheet, sr, er, sc, ec)
    except Exception:
        pass

    # 4) ESCALA / AJUSTE — zere tudo e ative só UM modo
    stx = os.getenv("SCALE_TO_PAGES_X", "1").strip()
    sty = os.getenv("SCALE_TO_PAGES_Y", "0").strip()
    page_scale = os.getenv("PAGE_SCALE", "").strip()

    try:
        if hasattr(ps, "ScaleToPages"):
            ps.ScaleToPages = 0
        ps.ScaleToPagesX = 0
        ps.ScaleToPagesY = 0
        ps.PageScale     = 100
    except Exception:
        pass

    # 4a) Modo AUTO (PAGE_SCALE=AUTO) — calcula PageScale e desliga fit
    if page_scale.upper() == "AUTO":
        try:
            # área usada
            cur = sheet.createCursor()
            try: cur.gotoStartOfUsedArea(False)
            except: pass
            try: cur.gotoEndOfUsedArea(True)
            except: pass
            addr = cur.RangeAddress
            srU, erU = int(addr.StartRow),   int(addr.EndRow)
            scU, ecU = int(addr.StartColumn),int(addr.EndColumn)

            # tamanho do conteúdo (1/100 mm -> mm)
            cols, rows = sheet.Columns, sheet.Rows
            content_w_100 = sum(cols.getByIndex(c).Width  for c in range(scU, ecU + 1))
            content_h_100 = sum(rows.getByIndex(r).Height for r in range(srU, erU + 1))
            content_w_mm = max(1.0, content_w_100 / 100.0)
            content_h_mm = max(1.0, content_h_100 / 100.0)

            # página útil (mm)
            fmt = os.getenv("PAGE_FORMAT", "A4").upper()
            ori = os.getenv("PAGE_ORIENTATION", "PORTRAIT").upper()
            sizes = {"A4": (210.0, 297.0), "A3": (297.0, 420.0), "LETTER": (215.9, 279.4)}
            pw, ph = sizes.get(fmt, sizes["A4"])
            if ori == "LANDSCAPE":
                pw, ph = ph, pw
            mt = float(os.getenv("MARGIN_TOP_MM",    "8"))
            mb = float(os.getenv("MARGIN_BOTTOM_MM", "8"))
            ml = float(os.getenv("MARGIN_LEFT_MM",   "8"))
            mr = float(os.getenv("MARGIN_RIGHT_MM",  "8"))
            avail_w = max(10.0, pw - ml - mr)
            avail_h = max(10.0, ph - mt - mb)

            # melhor escala que caiba em w e h
            scale_w = avail_w / content_w_mm
            scale_h = avail_h / content_h_mm
            scale   = max(0.1, min(2.0, min(scale_w, scale_h)))
            scale_pct = max(10, min(200, int(scale * 100) - 2))  # folga

            ps.PageScale     = scale_pct
            ps.ScaleToPagesX = 0
            ps.ScaleToPagesY = 0
            if hasattr(ps, "ScaleToPages"):
                ps.ScaleToPages = 0

            # opcional: se tiver helper para preencher altura
            try:
                content_h_after_mm = content_h_mm * (scale_pct / 100.0)
                slack = avail_h - content_h_after_mm
                if slack > 0.06 * (avail_h + content_h_after_mm) / 2.0:
                    _expand_rows_to_fill_height(sheet, srU, erU, avail_h, content_h_after_mm, safety=0.985)
            except Exception:
                pass

        except Exception:
            pass

    # 4b) % fixo (PAGE_SCALE numérico)
    elif page_scale:
        try:
            ps.PageScale     = max(10, min(200, int(float(page_scale))))
            ps.ScaleToPagesX = 0
            ps.ScaleToPagesY = 0
            if hasattr(ps, "ScaleToPages"):
                ps.ScaleToPages = 0
        except Exception:
            pass

    # 4c) Fit (padrão: 1x0 → caber na largura, altura livre)
    else:
        try:
            ps.ScaleToPagesX = int(stx or "1")
            ps.ScaleToPagesY = 0 if (sty == "" or sty == "0") else int(sty)
            if hasattr(ps, "ScaleToPages"):
                ps.ScaleToPages = 0
        except Exception:
            pass

    # 5) Títulos (linhas do topo) e não reotimizar col/lin
    if title_rows:
        try: sheet.TitleRows = title_rows
        except Exception: pass
    try:
        sheet.Columns.OptimalWidth = False
        sheet.Rows.OptimalHeight  = False
    except Exception:
        pass

    # 6) recálculo e LOG (depois de aplicar tudo)
    try:
        doc.calculateAll()
        import sys
        print(
            f"[UNO] fmt={os.getenv('PAGE_FORMAT','A4')} ori={os.getenv('PAGE_ORIENTATION','PORTRAIT')}"
            f" margins(mm)=({os.getenv('MARGIN_TOP_MM','?')},{os.getenv('MARGIN_RIGHT_MM','?')},"
            f"{os.getenv('MARGIN_BOTTOM_MM','?')},{os.getenv('MARGIN_LEFT_MM','?')})"
            f" fit=({getattr(ps,'ScaleToPagesX',None)},{getattr(ps,'ScaleToPagesY',None)})"
            f" ScaleToPages={getattr(ps,'ScaleToPages',None) if hasattr(ps,'ScaleToPages') else None}"
            f" pageScale={getattr(ps,'PageScale',None)}",
            file=sys.stdout, flush=True
        )
    except Exception:
        pass


# -------------------- Preenchimento do template --------------------

def _replace_tokens(sheet, tokens: dict):
    # Varre uma área suficiente; ajuste se seu template usar mais col/linhas
    max_r, max_c = 120, 200
    for r in range(max_r):
        for c in range(max_c):
            cell = sheet.getCellByPosition(c, r)  # 0-based
            txt = cell.String or ""
            if txt:
                new_txt = txt
                for k, v in tokens.items():
                    new_txt = new_txt.replace(k, v or "")
                if new_txt != txt:
                    cell.String = new_txt

def _find_header_cols(sheet) -> dict:
    # procura pelos títulos na área 25x25 (ajuste se necessário)
    for r in range(0, 25):
        names = {}
        for c in range(0, 25):
            txt = (sheet.getCellByPosition(c, r).String or "").strip()
            if txt:
                names[txt] = c
        if all(x in names for x in COLS):
            return {x: names[x] for x in COLS}
    raise RuntimeError("Cabeçalho da tabela não encontrado. Verifique o template e os títulos das colunas.")

def _fill_table(sheet, cols_map: dict, produtos_slice):
    r0 = ROWS_START - 1  # UNO é 0-based
    for idx, item in enumerate(produtos_slice):
        r = r0 + idx
        sheet.getCellByPosition(cols_map["Ocorrência"], r).String = item["ocorrencia"]
        sheet.getCellByPosition(cols_map["RAT"], r).String = item.get("rat", "") or ""
        sheet.getCellByPosition(cols_map["Qtde"], r).Value = float(item["qtde"])
        sheet.getCellByPosition(cols_map["Nota Fiscal"], r).String = str(item["numero_nf"])
        sheet.getCellByPosition(cols_map["Código"], r).String = item["codigo_prod"]
        sheet.getCellByPosition(cols_map["Valor NF"], r).String = formatar_valor(item["valor_nf"])

# -------------------- Função pública --------------------

def preencher_e_exportar_lote(qlid: str, cidade: str, header: dict,
                              produtos, data_iso: str,
                              volumes: int, out_pdf_path: str):

    # ordena produtos por número de NF numérico
    produtos = sorted(produtos, key=lambda x: int("0" + "".join(filter(str.isdigit, str(x["numero_nf"])))))
    dt = datetime.fromisoformat(data_iso)
    total_nf = sum(p["valor_nf"] for p in produtos)

    tokens = {
        "{{LOCAL}}": cidade.upper(),
        "{{DIA}}": f"{dt.day:02d}",
        "{{MES}}": MESES[dt.month].upper(),
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
        "{{TOTAL_VALOR_NF}}": formatar_valor(total_nf),
        "{{DEBUG}}": "Rodrigo testando às " + datetime.now().strftime("%H:%M"),
    }

    pages = max(1, math.ceil(len(produtos) / ROWS_PER_PAGE))
    tmpdir = tempfile.mkdtemp(prefix="minuta_")
    pdfs = []

    # overlay do logo — lido 1x por eficiência
    logo_path = os.getenv("LOGO_HEADER_PATH")
    x_mm   = float(os.getenv("LOGO_X_MM", "15"))
    top_mm = float(os.getenv("LOGO_TOP_MM", "10"))
    w_mm   = float(os.getenv("LOGO_WIDTH_MM", "65"))
    align  = os.getenv("LOGO_ALIGN", "left")
    margin_mm = float(os.getenv("LOGO_MARGIN_MM", "15"))

    try:
        for i in range(pages):
            # abre template e planilha
            doc = _open_template(TEMPLATE_PATH)
            sheet = doc.Sheets.getByIndex(0)

            # substitui tokens e preenche tabela
            _replace_tokens(sheet, tokens)
            cols = _find_header_cols(sheet)
            slice_i = produtos[i*ROWS_PER_PAGE:(i+1)*ROWS_PER_PAGE]
            _fill_table(sheet, cols, slice_i)

            # limita área de impressão e ajusta layout (A4, margens, escala)
            #bounds = _bounds_from_cols(cols, ROWS_START, len(slice_i))
            #_tunar_pagina(doc, title_rows="$1:$3", bounds=bounds)
            _tunar_pagina(doc, title_rows="$1:$3")

            # exporta PDF cru
            page_pdf_raw = os.path.join(tmpdir, f"page_{i+1}.pdf")
            _export_pdf(doc, page_pdf_raw)
            doc.close(True)

            # overlay opcional do logo
            page_pdf_final = page_pdf_raw
            if logo_path and os.path.exists(logo_path):
                page_pdf_with_logo = os.path.join(tmpdir, f"page_{i+1}__logo.pdf")
                try:
                    overlay_logo_on_pdf(page_pdf_raw, page_pdf_with_logo, logo_path, x_mm, top_mm, w_mm, align=align, margin_mm=margin_mm)
                except TypeError:
                    # fallback p/ versões antigas de overlay_logo_on_pdf sem 'align'
                    overlay_logo_on_pdf(page_pdf_raw, page_pdf_with_logo, logo_path, x_mm, top_mm, w_mm)
                page_pdf_final = page_pdf_with_logo

            pdfs.append(page_pdf_final)

        # junta todas as páginas
        merge_pdfs(pdfs, out_pdf_path)
    finally:
        # temporários serão limpos pelo SO quando possível
        pass
