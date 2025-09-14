#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="/home/pi/bot-copilot/.env"
ART_DIR="/home/pi/bot-copilot/app/data/rat_artifacts"
RET_DAYS=7

if [ -f "$ENV_FILE" ]; then
  set -a; source "$ENV_FILE"; set +a
  ART_DIR="${RAT_ARTIFACTS_DIR:-$ART_DIR}"
  RET_DAYS="${RAT_ARTIFACTS_RETENTION_DAYS:-$RET_DAYS}"
fi

mkdir -p "$ART_DIR"
find "$ART_DIR" -type f -mtime +"$RET_DAYS" -delete || true
echo "[CLEAN] $ART_DIR -> >${RET_DAYS}d removidos"
