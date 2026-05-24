#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start-node.sh  — inicia um nó do DistributedLock como processo local
#
# Uso:
#   ./scripts/start-node.sh <node-name> <port> <seeds>
#
# Exemplos:
#   ./scripts/start-node.sh node1 8081 "http://localhost:8082,http://localhost:8083"
#   ./scripts/start-node.sh node2 8082 "http://localhost:8081,http://localhost:8083"
#   ./scripts/start-node.sh node3 8083 "http://localhost:8081,http://localhost:8082"
#
# O log é escrito em logs/<node-name>.log e lido automaticamente pelo Promtail.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NODE_NAME="${1:?Informe o nome do nó, ex: node1}"
PORT="${2:?Informe a porta, ex: 8081}"
SEEDS="${3:?Informe as seeds, ex: http://localhost:8082,http://localhost:8083}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR="$PROJECT_DIR/build/libs/distributedlock-0.0.1-SNAPSHOT.jar"
LOG_DIR="$PROJECT_DIR/logs"
PID_DIR="$PROJECT_DIR/.pids"

if [[ ! -f "$JAR" ]]; then
  echo "❌  JAR não encontrado: $JAR"
  echo "   Rode primeiro:  ./gradlew bootJar"
  exit 1
fi

mkdir -p "$LOG_DIR" "$PID_DIR"

PID_FILE="$PID_DIR/$NODE_NAME.pid"

if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "⚠️   $NODE_NAME já está rodando (PID $(cat "$PID_FILE"))"
  exit 0
fi

echo "🚀  Iniciando $NODE_NAME na porta $PORT..."
echo "    Seeds: $SEEDS"
echo "    Log:   $LOG_DIR/$NODE_NAME.log"

NODE_NAME="$NODE_NAME" \
NODE_URL="http://localhost:$PORT" \
SERVER_PORT="$PORT" \
RAFT_SEEDS="$SEEDS" \
  java -jar "$JAR" \
  >> "$LOG_DIR/$NODE_NAME.stdout.log" 2>&1 &

echo $! > "$PID_FILE"
echo "✅  $NODE_NAME iniciado (PID $(cat "$PID_FILE"))"

