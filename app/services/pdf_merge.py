# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Iterable, Union

def merge_pdfs(input_paths: Iterable[Union[str, Path]], output_path: Union[str, Path]) -> str:
    """
    Junta vários PDFs (na ordem dada) em um único PDF.
    Ignora entradas inexistentes silenciosamente.
    Retorna o caminho do PDF final.
    """
    from pypdf import PdfReader, PdfWriter

    writer = PdfWriter()
    for p in input_paths:
        p = Path(p)
        if not p.exists():
            continue
        reader = PdfReader(str(p))
        for page in reader.pages:
            writer.add_page(page)

    outp = Path(output_path)
    outp.parent.mkdir(parents=True, exist_ok=True)
    with outp.open("wb") as f:
        writer.write(f)
    return str(outp)
