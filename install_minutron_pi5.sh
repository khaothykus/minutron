#!/usr/bin/env bash
set -euo pipefail

# Modo headless p/ Firefox e LibreOffice (evita "no DISPLAY")
export MOZ_HEADLESS=1
export SAL_USE_VCLPLUGIN=headless

APP_DIR="/home/pi/minutron"
PYTHON_BIN="python3.11"   # usa 3.11 do Debian Bookworm
VENV_DIR="${APP_DIR}/.venv"

echo "==> Instalando dependências do sistema…"
sudo apt-get update
sudo apt-get install -y \
  python3-venv python3-pip ${PYTHON_BIN} locales \
  libreoffice-core libreoffice-calc libreoffice-writer \
  python3-uno uno-libs-private \
  firefox-esr ca-certificates fonts-dejavu fonts-liberation

# Checagens silenciosas (sem DISPLAY)
firefox-esr -headless --version >/dev/null 2>&1 || true
libreoffice --headless --version >/dev/null 2>&1 || true

echo "==> Preparando diretórios…"
mkdir -p "${APP_DIR}/data" "${APP_DIR}/app/templates"
touch "${APP_DIR}/data/.keep" "${APP_DIR}/app/templates/.keep"

echo "==> Preparando venv (com system-site-packages p/ UNO)…"
rm -rf "${VENV_DIR}"
${PYTHON_BIN} -m venv "${VENV_DIR}" --system-site-packages
source "${VENV_DIR}/bin/activate"
pip install -U pip wheel

echo "==> Instalando requirements…"
# Garante deps essenciais no requirements, se não estiverem
REQ="${APP_DIR}/requirements.txt"
grep -qxF 'reportlab'                 "$REQ" || echo 'reportlab' >> "$REQ"
grep -qxF 'pypdf'                     "$REQ" || echo 'pypdf' >> "$REQ"
grep -qxF 'pdfplumber'                "$REQ" || echo 'pdfplumber' >> "$REQ"
grep -qxF 'pdfminer.six'              "$REQ" || echo 'pdfminer.six' >> "$REQ"
grep -qxF 'pillow'                    "$REQ" || echo 'pillow' >> "$REQ"
grep -qxF 'selenium'                  "$REQ" || echo 'selenium' >> "$REQ"
grep -qxF 'python-telegram-bot[job-queue]' "$REQ" || echo 'python-telegram-bot[job-queue]' >> "$REQ"
grep -qxF 'sdnotify'                  "$REQ" || echo 'sdnotify' >> "$REQ"
pip install -U -r "$REQ"
deactivate

echo "==> Gerando .env se não existir…"
if [ ! -f "${APP_DIR}/.env" ]; then
  cat > "${APP_DIR}/.env" <<'ENV'
BOT_TOKEN=
ADMIN_TELEGRAM_ID=

BASE_DATA_DIR=/home/pi/minutron/data
TEMPLATE_PATH=/home/pi/minutron/app/templates/minuta_template.xlsx

# Selenium/Firefox
FIREFOX_BINARY=/usr/bin/firefox-esr
GECKODRIVER_PATH=/usr/local/bin/geckodriver
MOZ_HEADLESS=1

# RAT Scraper
RAT_URL=https://servicos.ncratleos.com/consulta_ocorrencia/start.swe
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

# Página / impressão
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

# Logo overlay
LOGO_HEADER_PATH=/home/pi/minutron/app/templates/logo.png
LOGO_ALIGN=
LOGO_WIDTH_MM=35
LOGO_TOP_MM=30
LOGO_MARGIN_MM=30

# Locale/timezone
LOG_LEVEL=INFO
LC_ALL=pt_BR.UTF-8
LANG=pt_BR.UTF-8
TZ=America/Sao_Paulo

# LibreOffice/UNO headless
SAL_USE_VCLPLUGIN=headless
ENV
fi
sudo dos2unix "${APP_DIR}/.env" >/dev/null 2>&1 || true
sudo chmod 640 "${APP_DIR}/.env"

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
# Se você habilitar watchdog no serviço, lembre de ter job-queue ativo no bot
# WatchdogSec=0

ExecStart=${VENV_DIR}/bin/python ${APP_DIR}/app/bot.py
Restart=on-failure
RestartSec=3
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT

echo "==> Dando reload e habilitando serviço…"
sudo systemctl daemon-reload
sudo systemctl enable minutron

echo "==> Pronto. Agora rode: sudo systemctl start minutron && journalctl -u minutron -f"
