import re

# Padrões principais usados em todo o parser
RX_OCORR = re.compile(r"OCORR:\s*([A-Z]{2}\d{8})")
RX_STATUS = re.compile(r"\*{3}\s*(BOM|RUIM|DOA)\s*\*{3}", re.I)
RX_NUMNF = re.compile(r"(?:N[ºO]|NO\.)\s*(\d{6,})", re.I | re.S)
RX_CNPJ = re.compile(r"\b(\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2})\b")
RX_CPF  = re.compile(r"\b(\d{3}\.?\d{3}\.?\d{3}-?\d{2})\b")
RX_CEP  = re.compile(r"\b(\d{5}-?\d{3})\b")
RX_UF   = re.compile(r"\b(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)\b")
RX_IE   = re.compile(r"Inscri[çc][aã]o\s*Estadual[: ]+([A-Z0-9\.-/]+)", re.I)
RX_TRANSP = re.compile(r"Transportadora[: ]+(.+)", re.I)
RX_END = re.compile(r"Endere[çc]o[: ]+(.+)", re.I)
RX_REMETENTE = re.compile(r"NOME/RAZÃO SOCIAL\s+([A-ZÀ-Ú\s]+)\s+CNPJ/CPF\s+([\d\.\-\/]+)", re.I)
RX_TRANSPORTADOR = re.compile(r"NOME/RAZÃO SOCIAL\s*\n([A-ZÀ-Ú\s&]+TRANSP[A-ZÀ-Ú\s&]+)", re.I)
