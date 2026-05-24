#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# stop-node.sh  — para um nó local (simula falha de nó / crash)
#
# Uso:
#   ./scripts/stop-node.sh <node-name>           # SIGTERM (graceful)
#   ./scripts/stop-node.sh <node-name> --kill    # SIGKILL (crash imediato)
#
# Exemplos de cenários de demo:
#   ./scripts/stop-node.sh node2              # para o nó graciosamente
#   ./scripts/stop-node.sh node2 --kill       # simula crash abrupto
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NODE_NAME="${1:?Informe o nome do nó, ex: node2}"
MODE="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/../.pids"
PID_FILE="$PID_DIR/$NODE_NAME.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "⚠️   Nenhum PID encontrado para $NODE_NAME (talvez já esteja parado)"
  exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
  echo "⚠️   Processo $PID não está mais rodando"
  rm -f "$PID_FILE"
  exit 0
fi

if [[ "$MODE" == "--kill" ]]; then
  echo "💥  Matando $NODE_NAME abruptamente (PID $PID) — SIGKILL"
  kill -9 "$PID"
else
  echo "🛑  Parando $NODE_NAME graciosamente (PID $PID) — SIGTERM"
  kill "$PID"
fi

rm -f "$PID_FILE"
echo "✅  $NODE_NAME parado"

