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
