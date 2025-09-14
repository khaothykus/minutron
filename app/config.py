# app/config.py — Linux/RPi5 friendly (no hardcoded /app)
# Ajusta diretórios via env e tem defaults seguros.

from __future__ import annotations
import os
from pathlib import Path

# --- Diretórios base ---
# Raiz do projeto
APP_DIR = Path(os.environ.get("APP_DIR", Path(__file__).resolve().parents[1]))

# Diretório de dados (onde ficam users/logs/saídas)
BASE_DATA_DIR = Path(os.environ.get("OUTPUT_DIR", APP_DIR / "data"))
BASE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Subpastas comuns
(DATA_USERS := BASE_DATA_DIR / "users").mkdir(parents=True, exist_ok=True)
(DATA_LOGS := BASE_DATA_DIR / "logs").mkdir(parents=True, exist_ok=True)

# Template XLSX com logo no Header/Footer (pode vir do env)
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", str(APP_DIR / "templates" / "template.xlsx"))

# --- Configurações de bot/ambiente ---
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN não definido. Configure no .env ou variáveis de ambiente.")

ADMIN_TELEGRAM_ID = os.environ.get("ADMIN_TELEGRAM_ID")
if ADMIN_TELEGRAM_ID:
    try:
        ADMIN_TELEGRAM_ID = int(ADMIN_TELEGRAM_ID)
    except ValueError:
        raise RuntimeError("ADMIN_TELEGRAM_ID deve ser inteiro.")
else:
    ADMIN_TELEGRAM_ID = None

ENV = os.environ.get("ENV", "prod")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Exporte o que outros módulos esperam
__all__ = [
    "APP_DIR",
    "BASE_DATA_DIR",
    "DATA_USERS",
    "DATA_LOGS",
    "TEMPLATE_PATH",
    "BOT_TOKEN",
    "ADMIN_TELEGRAM_ID",
    "ENV",
    "LOG_LEVEL",
]
