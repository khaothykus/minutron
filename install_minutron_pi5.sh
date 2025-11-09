#!/usr/bin/env bash
set -euo pipefail

# Modo headless p/ Firefox e LibreOffice
export MOZ_HEADLESS=1
export SAL_USE_VCLPLUGIN=headless

APP_DIR="/home/pi/minutron"

# === detectar o melhor python disponível ===
PY_CANDIDATES=(
  /usr/bin/python3.13
  /usr/bin/python3.12
  /usr/bin/python3.11
  "$(command -v python3 || true)"
)
PYTHON_BIN=""
for c in "${PY_CANDIDATES[@]}"; do
  if [ -n "$c" ] && [ -x "$c" ]; then
    PYTHON_BIN="$c"
    break
  fi
done

if [ -z "$PYTHON_BIN" ]; then
  echo "ERRO: não achei python3 no sistema." >&2
  exit 1
fi

VENV_DIR="${APP_DIR}/.venv"

GECKO_VERSION="v0.35.0"
GECKO_INSTALL_PATH="/usr/local/bin/geckodriver"

echo "==> Instalando dependências do sistema…"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3 python3-venv python3-pip locales \
  libreoffice-core libreoffice-calc libreoffice-writer \
  python3-uno uno-libs-private \
  firefox-esr ca-certificates fonts-dejavu fonts-liberation \
  fonts-crosextra-carlito fonts-crosextra-caladea \
  curl wget tar gzip unzip dos2unix

# Checagens silenciosas
firefox-esr -headless --version >/dev/null 2>&1 || true
libreoffice --headless --version >/dev/null 2>&1 || true

echo "==> Preparando diretórios…"
mkdir -p "${APP_DIR}/data" "${APP_DIR}/app/templates"
touch "${APP_DIR}/data/.keep" "${APP_DIR}/app/templates/.keep"

echo "==> Preparando venv (com system-site-packages p/ UNO)…"
rm -rf "${VENV_DIR}"
"$PYTHON_BIN" -m venv "${VENV_DIR}" --system-site-packages
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"
pip install -U pip wheel

echo "==> Instalando requirements…"
REQ="${APP_DIR}/requirements.txt"
if [ ! -f "$REQ" ]; then
  echo "# requirements for minutron" > "$REQ"
fi

# garante libs que o teu bot usa
grep -qxF 'reportlab'                      "$REQ" || echo 'reportlab' >> "$REQ"
grep -qxF 'pypdf'                          "$REQ" || echo 'pypdf' >> "$REQ"
grep -qxF 'pdfplumber'                     "$REQ" || echo 'pdfplumber' >> "$REQ"
grep -qxF 'pdfminer.six'                   "$REQ" || echo 'pdfminer.six' >> "$REQ"
grep -qxF 'pillow'                         "$REQ" || echo 'pillow' >> "$REQ"
grep -qxF 'selenium'                       "$REQ" || echo 'selenium' >> "$REQ"
grep -qxF 'python-telegram-bot[job-queue]' "$REQ" || echo 'python-telegram-bot[job-queue]' >> "$REQ"
grep -qxF 'sdnotify'                       "$REQ" || echo 'sdnotify' >> "$REQ"

pip install -U -r "$REQ"
deactivate

echo "==> Gerando .env se não existir…"
if [ ! -f "${APP_DIR}/.env" ]; then
  cat > "${APP_DIR}/.env" <<'ENV'
# TELEGRAM
BOT_TOKEN=
ADMIN_TELEGRAM_ID=
# Se quiser mais de um admin separado por vírgula:
# ADMIN_TELEGRAM_IDS=123,456

# DIRS
BASE_DATA_DIR=/home/pi/minutron/data
TEMPLATE_PATH=/home/pi/minutron/app/templates/minuta_template.xlsx

# DANFE_EMITENTE_REGEX=NCR\s+BRASIL\s+LTDA

# Painel: mostra "✅ Lote finalizado" e apaga depois de 20s (recomendado)
PANEL_CLEANUP_MODE=finalize
PANEL_CLEANUP_TTL=20

#OU

# Apaga o painel imediatamente ao final
# PANEL_CLEANUP_MODE=delete

# OU

# Mantém o painel no chat (não apaga nem troca o texto)
#PANEL_CLEANUP_MODE=keep

# Impressão A4 via CUPS
PRINT_ENABLE=1
PRINT_PRINTER_NAME=EPSON_L4150
PRINT_COPIES=1
PRINT_OPTIONS=media=A4
PRINT_AUTO=0

# deixa tudo pronto pra não cortar bordas
PRINT_ADD_MARGIN_MM=
PRINT_FIT_TO_PAGE=1

# ——— admins (user_id e/ou chat_id, separados por vírgula) ———
PRINT_ADMIN_CHAT_IDS=

# Merge (minuta + DANFEs)
MERGE_DANFES_WITH_MINUTA=1

# /print PDF solto
PRINT_ANY_PDF_ENABLE=0
PRINT_MAX_FILE_MB=20

# SELENIUM / RAT
FIREFOX_BINARY=/usr/bin/firefox-esr
GECKODRIVER_PATH=/usr/local/bin/geckodriver
RAT_URL=https://servicos.ncratleos.com/consulta_ocorrencia/start.swe
MOZ_HEADLESS=1
RAT_HEADLESS=1
RAT_PAGELOAD_TIMEOUT=35
RAT_STEP_TIMEOUT=25
RAT_FLOW_TIMEOUT=90
RAT_RESULT_STABILIZE_MS=900
RAT_DETAIL_EXTRA_WAIT=7
RAT_DEEP_SCAN=1
RAT_MAX_RATS_PER_OCC=80
RAT_NAV_TIMEOUT_S=15
RAT_STEP_TIMEOUT_S=12
RAT_TOTAL_TIMEOUT_S=60
RAT_NETWORK_IDLE_S=2.5
RAT_SAVE_ARTIFACTS=0
RAT_ARTIFACTS_DIR=/home/pi/minutron/data/rat_artifacts

# Concurrency
# 0 = auto (reservado p/ patch 2/2 de parse paralelo)
PARSE_WORKERS=0
# pode testar 6~8 se o Pi estiver folgado
RAT_CONCURRENCY=6
INGEST_CONCURRENCY=6
DANFE_CACHE_ENABLED=1
DANFE_CACHE_DIR=/home/pi/minutron/data/cache/danfe

PREVIEW_ENABLED=1
PREVIEW_PAGES=1
PREVIEW_DPI=90

# LOGO overlay
LOGO_HEADER_PATH=/home/pi/minutron/app/templates/logo.png
LOGO_ALIGN=
LOGO_WIDTH_MM=35
LOGO_TOP_MM=30
LOGO_MARGIN_MM=30

# Página (UNO)
PAGE_FORMAT=A4
PAGE_ORIENTATION=PORTRAIT
MARGIN_TOP_MM=12
MARGIN_BOTTOM_MM=8
MARGIN_LEFT_MM=16
MARGIN_RIGHT_MM=8
PAGE_SCALE=
CENTER_H=1
CENTER_V=0
SCALE_TO_PAGES_X=1
SCALE_TO_PAGES_Y=0

# Locale/TZ
LOG_LEVEL=INFO
LC_ALL=pt_BR.UTF-8
LANG=pt_BR.UTF-8
TZ=America/Sao_Paulo

# --- ETIQUETAS (Elgin L42 Pro) ---
LABELS_ENABLED=1
LABEL_ADMIN_ONLY=1

# Saída (USB direto; deixe LABEL_PRINTER vazio se não for usar CUPS)
# LABEL_DEVICE=/dev/usb/lp0
LABEL_DEVICE=
# ou se for CUPS:
LABEL_PRINTER=ELGIN_L42PRO_FULL

# Tamanho de mídia
LABEL_WIDTH_MM=94
LABEL_HEIGHT_MM=70
LABEL_GAP_MM=2

# 1 = imprime "de baixo pra cima" (o que funcionou pra você)
LABEL_DIRECTION=1

# Qualidade/velocidade
LABEL_SPEED=4
LABEL_DENSITY=12

# Fonte TSPL
LABEL_FONT_NAME=4
LABEL_FONT_SCALE=1

# Centralização horizontal dos TEXTOS (negativo = vai pra ESQUERDA; positivo = DIREITA)
LABEL_TEXT_CENTER_OFFSET_MM=-11.0

# Alturas (a partir do TOPO da etiqueta)
LABEL_Y_SAP_MM=23                   # “CÓDIGO TÉCNICO SAP”
LABEL_Y_OCORR_MM=35                 # “Nº OCORRÊNCIA”
LABEL_Y_PECA_MM=49                  # “PEÇA RETIRADA”

# Linha dos status (um “X” no lugar certo)
LABEL_Y_STATUS_MM=65                # altura aprovada
LABEL_X_GOOD_MM=12                  # centros dos parênteses
LABEL_X_BAD_MM=34
LABEL_X_DOA_MM=57

# Conteúdos fixos/úteis
LABEL_CODIGO_TECNICO=20373280
LABEL_COPIES_PER_QTY=1              # 1 etiqueta por unidade (o bot multiplica pela qtde)

# Ajustes finos
SHIFT_X_GLOBAL=0
SHIFT_Y_GLOBAL=0
SHIFT_X_STATUS=0
SHIFT_Y_STATUS=0
SHIFT_X_COD_TEC=2
SHIFT_Y_COD_TEC=0
SHIFT_X_OCORR=0
SHIFT_Y_OCORR=0
SHIFT_X_PROD=0
SHIFT_Y_PROD=0

# Transportadora
# TRANSPORTADORA_PADRAO=RODONAVES TRANSP E ENCOMENDAS LTDA

# Modo:
# - always_env -> SEMPRE usar a TRANSPORTADORA_PADRAO na minuta
# - auto       -> usar lógica inteligente com base nas NFs + PADRAO
# TRANSPORTADORA_MODE=auto
ENV
fi
sudo dos2unix "${APP_DIR}/.env" >/dev/null 2>&1 || true
sudo chmod 640 "${APP_DIR}/.env"

###########################
# geckodriver
###########################
echo "==> Instalando geckodriver (${GECKO_VERSION})…"
if ! sudo apt-get install -y geckodriver >/dev/null 2>&1; then
  ARCH="$(uname -m)"
  case "$ARCH" in
    aarch64|arm64) WANT_PAT="linux-aarch64" ;;
    armv7l)        WANT_PAT="linux-arm7hf" ;;
    x86_64|amd64)  WANT_PAT="linux64" ;;
    *) echo "Arquitetura $ARCH não suportada automaticamente." >&2; exit 1 ;;
  esac
  TMPDIR="$(mktemp -d)"
  cd "$TMPDIR"
  ASSET_NAME="geckodriver-${GECKO_VERSION}-${WANT_PAT}.tar.gz"
  DOWNLOAD_URL="https://github.com/mozilla/geckodriver/releases/download/${GECKO_VERSION}/${ASSET_NAME}"
  curl -fsSLO "$DOWNLOAD_URL"
  tar -xzf "$ASSET_NAME"
  sudo mv -f geckodriver "$GECKO_INSTALL_PATH"
  sudo chmod +x "$GECKO_INSTALL_PATH"
  cd /
  rm -rf "$TMPDIR"
fi

# checa
geckodriver --version || true

echo "==> Instalando/atualizando unidade systemd…"
sudo tee /etc/systemd/system/minutron.service >/dev/null <<UNIT
[Unit]
Description=Minutron (Telegram Bot)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=pi
Group=pi
WorkingDirectory=${APP_DIR}/app
EnvironmentFile=${APP_DIR}/.env
Environment=PYTHONUNBUFFERED=1
Environment=MOZ_HEADLESS=1
ExecStart=${VENV_DIR}/bin/python ${APP_DIR}/app/bot.py
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
UNIT

echo "==> Dando reload e habilitando serviço…"
sudo systemctl daemon-reload
sudo systemctl enable minutron

echo "==> Pronto. Para iniciar agora: sudo systemctl start minutron && journalctl -u minutron -f"
