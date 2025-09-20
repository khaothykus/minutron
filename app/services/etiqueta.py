# /home/pi/minutron/app/services/etiqueta.py
import os
import time
import subprocess
from typing import Iterable, Dict, Any

# -------- helpers --------
def _env_str(name: str, default: str) -> str:
    # lê env e remove comentário no fim da linha
    val = os.getenv(name, default)
    if val is None:
        return default
    return val.split("#", 1)[0].strip()

def _env_bool(name: str, default: bool) -> bool:
    val = _env_str(name, "1" if default else "0").lower()
    return val in ("1", "true", "yes", "y", "on")

def _env_float(name: str, default: float) -> float:
    s = _env_str(name, str(default))
    try:
        return float(s)
    except Exception:
        return default

def _env_int(name: str, default: int) -> int:
    s = _env_str(name, str(default))
    try:
        return int(float(s))
    except Exception:
        return default

# -------- config (defaults iguais ao “95%” que funcionou) --------
LABEL_DEVICE = _env_str("LABEL_DEVICE", "/dev/usb/lp0")  # se vazio, tenta CUPS
LABEL_PRINTER = _env_str("LABEL_PRINTER", "")            # p/ usar lpr (-P)

LABEL_WIDTH_MM  = _env_float("LABEL_WIDTH_MM", 90.0)
LABEL_HEIGHT_MM = _env_float("LABEL_HEIGHT_MM", 70.0)
LABEL_GAP_MM    = _env_float("LABEL_GAP_MM", 2.0)
LABEL_DPI       = _env_int("LABEL_DPI", 203)
LABEL_FROM_BOTTOM = _env_bool("LABEL_FROM_BOTTOM", True)  # impressora puxa de baixo p/ cima

# Calibração que você aprovou (texto centralizado manualmente)
TEXT_CENTER_OFFSET_MM = _env_float("TEXT_CENTER_OFFSET_MM", -11.0)  # desloca todos os textos (− esq / + dir)
Y_UP = _env_float("Y_UP", 5.0)  # sobe tudo 5 mm

# Linhas (medidas a partir do TOPO da etiqueta!)
Y_SAP    = _env_float("Y_COD_TEC", 23.0) - Y_UP
Y_OCORR  = _env_float("Y_OCORR",   36.0) - Y_UP
Y_PECA   = _env_float("Y_PROD",    50.0) - Y_UP
# Altura dos X: 65 - 5 + 2.4 = 62.4
Y_STATUS = _env_float("Y_STATUS",  65.0) - Y_UP + 2.4

# Centros dos parênteses dos status
X_SHIFT = _env_float("X_SHIFT", -0.2)
# EXTRA_BAD_LEFT = _env_float("EXTRA_BAD_LEFT", 0.7)
X_GOOD = _env_float("X_STATUS_GOOD", 12.0) + X_SHIFT
# X_BAD  = _env_float("X_STATUS_BAD",  35.0) + X_SHIFT - EXTRA_BAD_LEFT
X_BAD  = _env_float("X_STATUS_BAD",  34.0) + X_SHIFT
X_DOA  = _env_float("X_STATUS_DOA",  57.0) + X_SHIFT

# Texto/Fontes (TSPL fonte "4" ~8x16 dots). Mantive escala 1, como no seu teste
FONT_W_DOTS = _env_int("LABEL_FONT_CHAR_WIDTH_DOTS", 8)  # “4” tem ~8 px de largura por char
TEXT_SCALE = _env_int("LABEL_TEXT_SCALE", 1)
STATUS_SCALE = _env_int("LABEL_STATUS_SCALE", 1)

# Valor fixo do código técnico
LABEL_CODIGO_TECNICO = _env_str("LABEL_CODIGO_TECNICO", "20373280")

# X que será impresso no status
STATUS_X_TEXT = _env_str("STATUS_X_TEXT", "X")

# Offsets globais finos (mm)
SHIFT_X_GLOBAL = _env_float("SHIFT_X_GLOBAL", 0.0)
SHIFT_Y_GLOBAL = _env_float("SHIFT_Y_GLOBAL", 0.0)

# Ajustes finos individuais
SHIFT_X_COD_TEC = _env_float("SHIFT_X_COD_TEC", 0.0)
SHIFT_Y_COD_TEC = _env_float("SHIFT_Y_COD_TEC", 0.0)
SHIFT_X_OCORR   = _env_float("SHIFT_X_OCORR", 0.0)
SHIFT_Y_OCORR   = _env_float("SHIFT_Y_OCORR", 0.0)
SHIFT_X_PROD    = _env_float("SHIFT_X_PROD", 0.0)
SHIFT_Y_PROD    = _env_float("SHIFT_Y_PROD", 0.0)
SHIFT_X_STATUS  = _env_float("SHIFT_X_STATUS", 0.0)
SHIFT_Y_STATUS  = _env_float("SHIFT_Y_STATUS", 0.0)

# Conversões
DPMM = LABEL_DPI / 25.4  # dots por mm
def dm(mm: float) -> int:
    return int(round(mm * DPMM))

def _tspl_header() -> list[str]:
    parts = [
        f"SIZE {LABEL_WIDTH_MM} mm,{LABEL_HEIGHT_MM} mm",
        f"GAP {LABEL_GAP_MM} mm,0 mm",
        "SPEED 4",
        "DENSITY 12",
        # DIRECTION 1 = impressão “de cabeça para baixo”, adequada p/ mídia que sai por baixo
        "DIRECTION 1" if LABEL_FROM_BOTTOM else "DIRECTION 0",
        "REFERENCE 0,0",
        "CLS",
    ]
    return parts

def _text_center_cmd(y_mm: float, s: str, scale: int = 1, extra_x_mm: float = 0.0) -> str:
    """
    Centraliza o texto manualmente com base no comprimento do texto em dots.
    Usa fonte TSPL "4" (~8x16), mantendo compatível com seu teste.
    """
    s = (s or "").replace('"', "'")
    w_dots = FONT_W_DOTS * scale * len(s)
    # Centro da etiqueta em mm + offset global + offset de centralização manual + ajuste fino
    x_mm_center_line = (LABEL_WIDTH_MM / 2.0) + TEXT_CENTER_OFFSET_MM + SHIFT_X_GLOBAL + extra_x_mm
    x = dm(x_mm_center_line) - (w_dots // 2)
    y = dm(y_mm + SHIFT_Y_GLOBAL)
    return f'TEXT {x},{y},"4",0,{scale},{scale},"{s}"'

def _put_x(x_mm: float, y_mm: float, scale: int = 1) -> str:
    x = dm(x_mm + SHIFT_X_GLOBAL + SHIFT_X_STATUS)
    y = dm(y_mm + SHIFT_Y_GLOBAL + SHIFT_Y_STATUS)
    return f'TEXT {x},{y},"4",0,{scale},{scale},"{STATUS_X_TEXT}"'

def _build_tspl(codigo_tecnico: str, ocorrencia: str, codigo_produto: str, status: str, copias: int = 1) -> bytes:
    status_norm = (status or "").strip().lower()
    parts = _tspl_header()

    # Linhas de texto centralizadas (com ajustes finos individuais X/Y em mm)
    parts.append(_text_center_cmd(Y_SAP + SHIFT_Y_COD_TEC,   codigo_tecnico or LABEL_CODIGO_TECNICO, TEXT_SCALE, extra_x_mm=SHIFT_X_COD_TEC))
    parts.append(_text_center_cmd(Y_OCORR + SHIFT_Y_OCORR,   ocorrencia or "",                          TEXT_SCALE, extra_x_mm=SHIFT_X_OCORR))
    parts.append(_text_center_cmd(Y_PECA + SHIFT_Y_PROD,     codigo_produto or "",                      TEXT_SCALE, extra_x_mm=SHIFT_X_PROD))

    # X no status correto
    if status_norm == "good":
        parts.append(_put_x(X_GOOD, Y_STATUS, STATUS_SCALE))
    elif status_norm == "bad":
        parts.append(_put_x(X_BAD,  Y_STATUS, STATUS_SCALE))
    elif status_norm == "doa":
        parts.append(_put_x(X_DOA,  Y_STATUS, STATUS_SCALE))
    else:
        # status desconhecido -> não marca nada
        pass

    parts.append(f"PRINT {max(1, int(copias))},1")
    tspl = "\r\n".join(parts) + "\r\n"
    return tspl.encode("latin-1", "ignore")

def _send_to_printer(payload: bytes) -> None:
    """
    Envia o TSPL para a impressora.
    - Se LABEL_DEVICE existir, escreve direto no dispositivo.
    - Senão, se LABEL_PRINTER estiver definido, usa 'lpr -P <nome>'.
    """
    dev = LABEL_DEVICE
    if dev and os.path.exists(dev):
        with open(dev, "wb") as f:
            f.write(payload)
        return

    printer = LABEL_PRINTER
    if printer:
        proc = subprocess.run(["/usr/bin/lpr", "-P", printer], input=payload, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"lpr retornou código {proc.returncode}")
        return

    # Sem device e sem printer -> erro claro
    raise RuntimeError("Nenhuma impressora configurada: defina LABEL_DEVICE ou LABEL_PRINTER no .env")

def imprimir_etiqueta(codigo_tecnico: str, ocorrencia: str, codigo_produto: str, status: str, copias: int = 1) -> None:
    """
    Imprime UMA etiqueta no layout aprovado:
      - 3 linhas centralizadas (código técnico, ocorrência, peça)
      - “X” no status (good|bad|doa) na linha inferior
    """
    payload = _build_tspl(
        codigo_tecnico=codigo_tecnico or LABEL_CODIGO_TECNICO,
        ocorrencia=ocorrencia or "",
        codigo_produto=codigo_produto or "",
        status=status or "",
        copias=max(1, int(copias or 1)),
    )
    # Opcional: gravar o TSPL num arquivo de debug
    if _env_bool("LABEL_DEBUG_TSPL", False):
        os.makedirs("/tmp/labels", exist_ok=True)
        with open(f"/tmp/labels/tspl_{int(time.time())}.txt", "wb") as dbg:
            dbg.write(payload)

    _send_to_printer(payload)

def print_batch(items: Iterable[Dict[str, Any]]) -> int:
    """
    Imprime uma lista de itens.
    Cada item deve ter: codigo_prod (ou 'codigo_produto'), ocorrencia, status, qtde (opcional).
    Retorna a quantidade total impressa.
    """
    total = 0
    for it in items:
        codigo_prod = it.get("codigo_prod") or it.get("codigo_produto") or ""
        ocorr = it.get("ocorrencia") or ""
        status = (it.get("status") or "").lower()
        qtde = int(it.get("qtde") or 1)
        copias = max(1, qtde * _env_int("LABEL_COPIES_PER_QTY", 1))
        imprimir_etiqueta(LABEL_CODIGO_TECNICO, ocorr, codigo_prod, status, copias=copias)
        total += copias
    return total
