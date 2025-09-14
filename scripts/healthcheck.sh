#!/usr/bin/env bash
set -euo pipefail

SERVICE="bot-copilot"
ENV_FILE="/home/pi/bot-copilot/.env"

# carrega BOT_TOKEN e TZ
if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
fi

# 1) processo ativo?
if ! systemctl is-active --quiet "$SERVICE"; then
  echo "[HC] $SERVICE inativo -> restart"
  systemctl restart "$SERVICE"
  exit 1
fi

# 2) token responde?
if [ -n "${BOT_TOKEN:-}" ]; then
  if ! curl -s --max-time 10 "https://api.telegram.org/bot${BOT_TOKEN}/getMe" | grep -q '"ok":true'; then
    echo "[HC] Telegram getMe falhou -> restart"
    systemctl restart "$SERVICE"
    exit 2
  fi
else
  echo "[HC] BOT_TOKEN ausente no .env"
fi

echo "[HC] ok"
exit 0
