from services.danfe_utils import _clean, is_danfe
from services.danfe_emitente import (
    extrair_emitente_basico,
    extrair_emitente_endereco,
    extrair_emitente_nome,
)
from services.danfe_remetente import (
    extrair_remetente,
    extrair_transportador,
)
from services.danfe_produtos import extrair_produtos_tabela
import pdfplumber
from services import transportadora_db


def extrair_danfe_completa(
    pdf_path: str,
    texto: str,
    tabelas: list,
) -> tuple[dict, list[dict]]:
    emitente = extrair_emitente_basico(texto)
    emitente.update(extrair_emitente_endereco(tabelas))
    emitente["nome_emitente"] = extrair_emitente_nome(tabelas)

    remetente = extrair_remetente(tabelas)
    emitente.update(remetente)

    emitente["transportador"] = extrair_transportador(tabelas)

    produtos = extrair_produtos_tabela(pdf_path, texto)
    return emitente, produtos


def parse_lote(pdf_paths: list[str]) -> tuple[dict, list[dict]]:
    """
    - Usa cabeçalho da primeira DANFE como base.
    - Soma produtos de todas.
    - Também coleta todas as transportadoras vistas no lote em:
        header["_transportadoras_lote"] = [..uniq..]
    """
    header: dict | None = None
    all_prod: list[dict] = []
    transportadoras: list[str] = []

    for i, path in enumerate(pdf_paths):
        with pdfplumber.open(path) as pdf:
            textos = []
            tabelas = []
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t:
                    textos.append(t)
                for tb in (page.extract_tables() or []):
                    tabelas.append(tb)
            texto = "\n".join(textos)

        emitente, produtos = extrair_danfe_completa(path, texto, tabelas)

        if header is None:
            header = emitente.copy()

        all_prod.extend(produtos)

        t = (emitente.get("transportador") or "").strip()
        if t:
            transportadoras.append(t)

    header = header or {}

    if transportadoras:
        uniq: list[str] = []
        for t in transportadoras:
            if t not in uniq:
                uniq.append(t)
        header["_transportadoras_lote"] = uniq

        if not header.get("transportador"):
            header["transportador"] = uniq[0]

        # alimenta o banco global
        transportadora_db.add_many(uniq)

    return header, all_prod


__all__ = ["extrair_danfe_completa", "is_danfe", "parse_lote"]
