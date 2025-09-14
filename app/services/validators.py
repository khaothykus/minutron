import re

_rx_qlid = re.compile(r"^[A-Z]{2}\d{6}$")
_rx_cidade = re.compile(r"^[A-Za-zÀ-ÿ ]+$")

def valida_qlid(s: str) -> bool:
    return bool(_rx_qlid.fullmatch(s.strip()))

def valida_cidade(s: str) -> bool:
    s = s.strip()
    return bool(s) and bool(_rx_cidade.fullmatch(s))

def valida_ocorrencia(s: str) -> bool:
    return bool(re.fullmatch(r"[A-Z]{2}\d{8}", s))
