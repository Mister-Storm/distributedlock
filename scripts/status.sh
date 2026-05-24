#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# status.sh  — mostra o status de todos os nós locais
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/../.pids"

echo "═══════════════════════════════════════"
echo "  DistributedLock — Status dos Nós"
echo "═══════════════════════════════════════"

if [[ ! -d "$PID_DIR" ]] || [[ -z "$(ls "$PID_DIR"/*.pid 2>/dev/null)" ]]; then
  echo "  Nenhum nó iniciado."
  exit 0
fi

for pid_file in "$PID_DIR"/*.pid; do
  NODE=$(basename "$pid_file" .pid)
  PID=$(cat "$pid_file")
  if kill -0 "$PID" 2>/dev/null; then
    STATUS="✅  rodando"
  else
    STATUS="❌  parado (PID obsoleto)"
  fi
  echo "  $NODE  PID=$PID  $STATUS"
done

echo "═══════════════════════════════════════"

