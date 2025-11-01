from services.danfe_utils import _clean, is_danfe
from services.danfe_emitente import (
    extrair_emitente_basico,
    extrair_emitente_endereco,
    extrair_emitente_nome
)
from services.danfe_remetente import (
    extrair_remetente,
    extrair_transportador
)
from services.danfe_produtos import extrair_produtos_tabela

from pdfminer.high_level import extract_text
import pdfplumber
def parse_lote(pdf_paths: list[str]) -> tuple[dict, list[dict]]:
    header = None
    all_prod = []

    for i, path in enumerate(pdf_paths):
        with pdfplumber.open(path) as pdf:
            texto = "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
            tabelas = [table for page in pdf.pages for table in page.extract_tables()]

        if i == 0:
            header, _ = extrair_danfe_completa(path, texto, tabelas)

        _, produtos = extrair_danfe_completa(path, texto, tabelas)
        all_prod.extend(produtos)

    return header or {}, all_prod


def extrair_danfe_completa(pdf_path: str, texto: str, tabelas: list) -> tuple[dict, list[dict]]:
    emitente = extrair_emitente_basico(texto)
    emitente.update(extrair_emitente_endereco(tabelas))
    emitente["nome_emitente"] = extrair_emitente_nome(tabelas)

    remetente = extrair_remetente(tabelas)
    emitente.update(remetente)

    emitente["transportador"] = extrair_transportador(tabelas)

    produtos = extrair_produtos_tabela(pdf_path, texto)

    return emitente, produtos

__all__ = ["extrair_danfe_completa", "is_danfe", "parse_lote"]


# ============================================================
# Parse paralelo de DANFEs (não-invasivo)
# Usa a própria parse_lote([pdf]) por arquivo, em paralelo,
# e agrega cabeçalho/produtos. Controlado por PARSE_WORKERS.
# ============================================================
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import PARSE_WORKERS
import os as _os
import traceback as _tb

def parse_lote_parallel(pdfs: list[str]):
    """
    Executa parse em paralelo (se PARSE_WORKERS > 1).
    Estratégia não-invasiva: para cada pdf, chama parse_lote([pdf]) e agrega.
    """
    try:
        workers_cfg = int(PARSE_WORKERS)
    except Exception:
        workers_cfg = 0

    if workers_cfg <= 1 or not pdfs or len(pdfs) == 1:
        return parse_lote(pdfs)

    max_workers = workers_cfg
    if max_workers <= 0:
        cpu = (_os.cpu_count() or 2)
        max_workers = min(len(pdfs), max(2, cpu * 2))
    else:
        max_workers = min(len(pdfs), max_workers)

    header_final = None
    produtos_agg: list[dict] = []

    # def _one(path: str):
    #     try:
    #         h, prods = parse_lote([path])
    #         return (h, prods, None)
    #     except Exception as e:
    #         _tb.print_exc()
    #         return (None, [], e)
    
    def _one(path: str):
        try:
            # >>> mudança aqui: usa cache <<<
            h, prods = parse_one_with_cache(path)
            return (h, prods, None)
        except Exception as e:
            _tb.print_exc()
            return (None, [], e)

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="danfe-parse") as ex:
        futs = {ex.submit(_one, p): p for p in pdfs}
        for fut in as_completed(futs):
            h, prods, err = fut.result()
            if h and not header_final:
                header_final = h
            if prods:
                produtos_agg.extend(prods)

    header_final = header_final or {}
    return header_final, produtos_agg

# =======================
# Mini-cache de parse
# =======================
import json as _json, hashlib as _hashlib
from config import DANFE_CACHE_ENABLED, DANFE_CACHE_DIR

def _sha256_file(path: str) -> str:
    h = _hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _cache_path_for(digest: str) -> str:
    # 1 arquivo .json por hash; conteúdo: {"header": {...}, "produtos": [...]}
    return str(DANFE_CACHE_DIR / f"{digest}.json")

def _cache_get(digest: str):
    if not DANFE_CACHE_ENABLED:
        return None
    p = _cache_path_for(digest)
    try:
        with open(p, "r", encoding="utf-8") as fp:
            data = _json.load(fp)
        # sanity
        if isinstance(data, dict) and "header" in data and "produtos" in data:
            return data
    except Exception:
        return None

def _cache_put(digest: str, header: dict, produtos: list[dict]):
    if not DANFE_CACHE_ENABLED:
        return
    p = _cache_path_for(digest)
    tmp = p + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fp:
            _json.dump({"header": header or {}, "produtos": produtos or []}, fp, ensure_ascii=False)
        import os as _os
        _os.replace(tmp, p)
    except Exception:
        pass

def parse_one_with_cache(path: str):
    """
    Usa parse_lote([path]) mas com cache por SHA256 do PDF.
    """
    digest = _sha256_file(path)
    got = _cache_get(digest)
    if got:
        return got["header"], got["produtos"]
    h, prods = parse_lote([path])  # chama seu parser já existente
    _cache_put(digest, h, prods)
    return h, prods

