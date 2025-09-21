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
  firefox-esr ca-certificates fonts-dejavu fonts-liberation \
  fonts-crosextra-carlito fonts-crosextra-caladea \
  curl wget tar

# Checagens silenciosas (sem DISPLAY)
firefox-esr -headless --version >/dev/null 2>&1 || true
libreoffice --headless --version >/dev/null 2>&1 || true

# =========================
# Instalação do geckodriver
# =========================
echo "==> Instalando geckodriver (apt ou fallback para release oficial)..."
if sudo apt-get install -y geckodriver >/dev/null 2>&1; then
  echo "-> geckodriver instalado via apt"
else
  echo "-> apt não trouxe geckodriver, tentando baixar release oficial..."

  # Detecta arquitetura e escolhe asset name
  ARCH="$(uname -m)"
  case "$ARCH" in
    aarch64|arm64) ASSET_ARCH="linux-aarch64" ;;
    armv7l)        ASSET_ARCH="linux-arm7hf" ;;
    x86_64|amd64)  ASSET_ARCH="linux64" ;;
    *) 
      echo "Arquitetura $ARCH não reconhecida automaticamente. Saindo."
      exit 1
      ;;
  esac

  # Busca última tag no GitHub releases e monta URL
  LATEST_JSON="$(curl -sSfL "https://api.github.com/repos/mozilla/geckodriver/releases/latest")" || {
    echo "Falha ao obter info do GitHub para geckodriver"; exit 1;
  }
  TAG="$(printf '%s\n' "$LATEST_JSON" | grep -m1 '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/')"
  if [ -z "$TAG" ]; then
    echo "Não foi possível detectar tag mais recente do geckodriver"; exit 1;
  fi

  ASSET_NAME="geckodriver-${TAG#v}-${ASSET_ARCH}.tar.gz"
  DOWNLOAD_URL="https://github.com/mozilla/geckodriver/releases/download/${TAG}/${ASSET_NAME}"

  echo "-> Tag detectada: $TAG"
  echo "-> Asset selecionado: $ASSET_NAME"
  echo "-> Baixando: $DOWNLOAD_URL"

  TMPDIR="$(mktemp -d)"
  pushd "$TMPDIR" >/dev/null

  if curl -fSLO "$DOWNLOAD_URL"; then
    tar -xzf "$ASSET_NAME"
    # extrai geckodriver e instala em /usr/local/bin
    if [ -f geckodriver ]; then
      sudo mv -f geckodriver /usr/local/bin/geckodriver
      sudo chmod +x /usr/local/bin/geckodriver
      echo "-> geckodriver instalado em /usr/local/bin/geckodriver"
    else
      echo "Arquivo geckodriver não encontrado após extrair o tarball"; popd >/dev/null; rm -rf "$TMPDIR"; exit 1
    fi
  else
    echo "Falha no download do geckodriver: $DOWNLOAD_URL"
    popd >/dev/null
    rm -rf "$TMPDIR"
    exit 1
  fi

  popd >/dev/null
  rm -rf "$TMPDIR"
fi

# Verifica versão instalada
if command -v geckodriver >/dev/null 2>&1; then
  echo "-> geckodriver --version: $(geckodriver --version | head -n1)"
else
  echo "geckodriver não está disponível no PATH após a instalação"
  exit 1
fi

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
# TELEGRAM
BOT_TOKEN=
ADMIN_TELEGRAM_ID=

# DIRS
BASE_DATA_DIR=/home/pi/minutron/data
TEMPLATE_PATH=/home/pi/minutron/app/templates/minuta_template.xlsx

# SELENIUM / RAT Scraper
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

# LOGO overlay (opcional)
LOGO_HEADER_PATH=/home/pi/minutron/app/templates/logo.png
LOGO_ALIGN=
LOGO_WIDTH_MM=35
LOGO_TOP_MM=30
LOGO_MARGIN_MM=30

# PÁGINA (UNO)
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

# --- ETIQUETAS (Elgin L42 Pro) - PERFIL 95% ---
LABELS_ENABLED=1
LABEL_ADMIN_ONLY=1                  # só pergunta pro ADMIN_TELEGRAM_ID

# Saída (USB direto; deixe LABEL_PRINTER vazio se não for usar CUPS)
LABEL_DEVICE=/dev/usb/lp0
LABEL_PRINTER=

# Mídia (tamanho real da sua etiqueta) + sentido de impressão
LABEL_WIDTH_MM=94
LABEL_HEIGHT_MM=70
LABEL_GAP_MM=2
LABEL_DIRECTION=1                   # 1 = imprime "de baixo pra cima" (o que funcionou pra você)

# Qualidade/velocidade
LABEL_SPEED=4
LABEL_DENSITY=12

# Fonte TSPL e escala (mantém o tamanho que ficou bom)
LABEL_FONT_NAME=4
LABEL_FONT_SCALE=1

# Centralização horizontal dos TEXTOS (negativo = vai pra ESQUERDA; positivo = DIREITA)
LABEL_TEXT_CENTER_OFFSET_MM=-11.0   # este valor te deu a centralização boa

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

# (Opcional) pequenos ajustes finos – deixe 0 enquanto estiver “95% ok”
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
