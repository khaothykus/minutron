def extrair_remetente(tabelas: list) -> dict:
    for table in tabelas:
        for row in table:
            if row and any("NOME/RAZÃO SOCIAL" in str(cell) for cell in row):
                nome_cell = next((cell for cell in row if cell and "NOME/RAZÃO SOCIAL" in cell), "")
                cpf_cell = next((cell for cell in row if cell and "CNPJ/CPF" in cell), "")
                nome = nome_cell.split("\n")[1].strip() if "\n" in nome_cell else ""
                cpf = cpf_cell.split("\n")[1].strip() if "\n" in cpf_cell else ""
                return {
                    "nome_remetente": nome,
                    "cpf_remetente": cpf
                }
    return {
        "nome_remetente": "",
        "cpf_remetente": ""
    }

def extrair_transportador(tabelas: list) -> str:
    for table in tabelas:
        for row in table:
            if row and any("TRANSP" in str(cell) for cell in row):
                cell = next((cell for cell in row if cell and "TRANSP" in cell), "")
                return cell.split("\n")[1].strip() if "\n" in cell else ""
    return ""
