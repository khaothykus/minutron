import re
from services.danfe_utils import _clean, formatar_ie
from services.danfe_regex import RX_CNPJ, RX_CEP, RX_UF

def formatar_cnpj(cnpj: str) -> str:
    cnpj = _clean(cnpj)
    if len(cnpj) == 14:
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
    return cnpj

def extrair_emitente_basico(txt: str) -> dict:
    nome_match = re.search(r"DANFE\s*\n(.+?)\s+Documento Auxiliar", txt, re.I)
    cnpj_match = RX_CNPJ.search(txt)
    ie_match = re.search(rf"{re.escape(cnpj_match.group(1))}\s+([A-Z0-9\.-/]+)", txt) if cnpj_match else None
    cep_match = RX_CEP.search(txt)
    uf_match = RX_UF.search(txt)
    cidade_match = re.search(rf"{cep_match.group(1)}\s*-\s*(.+?)\s*/", txt) if cep_match else None

    return {
        "nome_emitente": nome_match.group(1).strip() if nome_match else "",
        "cnpj_emitente": formatar_cnpj(cnpj_match.group(1)) if cnpj_match else "",
        "ie_emitente": formatar_ie(ie_match.group(1)) if ie_match else "",
        "cidade_emitente": cidade_match.group(1).strip() if cidade_match else "",
        "uf_emitente": uf_match.group(1) if uf_match else "",
        "cep_emitente": _clean(cep_match.group(1)) if cep_match else "",
    }

def extrair_emitente_endereco(tabelas: list) -> dict:
    for table in tabelas:
        for row in table:
            if row and isinstance(row[0], str) and "CEP" in row[0] and "FONE" in row[0]:
                linhas = row[0].split("\n")
                if len(linhas) >= 4:
                    rua_num = linhas[1].strip()
                    bairro = linhas[2].strip()
                    cep_cidade_uf = linhas[3].strip()

                    rua, numero = rua_num.split(",", 1)
                    cep_match = re.search(r"(\d{5}-?\d{3})", cep_cidade_uf)
                    cidade_uf_match = re.search(r"-\s*([A-ZÀ-Ú\s]+)\s*/\s*([A-Z]{2})", cep_cidade_uf)

                    return {
                        "rua_emitente": rua.strip(),
                        "numero_emitente": numero.strip(),
                        "bairro_emitente": bairro.strip(),
                        "cep_emitente": cep_match.group(1) if cep_match else "",
                        "cidade_emitente": cidade_uf_match.group(1).strip() if cidade_uf_match else "",
                        "uf_emitente": cidade_uf_match.group(2) if cidade_uf_match else "",
                    }
    return {
        "rua_emitente": "",
        "numero_emitente": "",
        "bairro_emitente": "",
        "cep_emitente": "",
        "cidade_emitente": "",
        "uf_emitente": "",
    }

def extrair_emitente_nome(tabelas: list) -> str:
    for table in tabelas:
        for row in table:
            if row and isinstance(row[0], str) and "AV" in row[0] and "CEP" in row[0]:
                return row[0].split("\n")[0].strip()
    return ""
