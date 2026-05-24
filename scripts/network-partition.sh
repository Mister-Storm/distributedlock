#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# network-partition.sh  — simula falha de rede entre dois nós usando iptables
#
# Uso:
#   ./scripts/network-partition.sh block   <porta-alvo>   # bloqueia tráfego
#   ./scripts/network-partition.sh unblock <porta-alvo>   # restaura tráfego
#
# Exemplos:
#   ./scripts/network-partition.sh block   8082   # isola node2 (porta 8082)
#   ./scripts/network-partition.sh unblock 8082   # restaura node2
#
# Requer: sudo / privilégios de root para manipular iptables
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ACTION="${1:?Informe a ação: block | unblock}"
PORT="${2:?Informe a porta do nó alvo, ex: 8082}"

case "$ACTION" in
  block)
    echo "🔥  Bloqueando tráfego de entrada e saída na porta $PORT (simulando partição de rede)..."
    sudo iptables -A INPUT  -p tcp --dport "$PORT" -j DROP
    sudo iptables -A OUTPUT -p tcp --sport "$PORT" -j DROP
    sudo iptables -A INPUT  -p tcp --sport "$PORT" -j DROP
    sudo iptables -A OUTPUT -p tcp --dport "$PORT" -j DROP
    echo "✅  Porta $PORT bloqueada — observe o Grafana: eleição deve ser disparada"
    ;;
  unblock)
    echo "🔓  Restaurando tráfego na porta $PORT..."
    sudo iptables -D INPUT  -p tcp --dport "$PORT" -j DROP 2>/dev/null || true
    sudo iptables -D OUTPUT -p tcp --sport "$PORT" -j DROP 2>/dev/null || true
    sudo iptables -D INPUT  -p tcp --sport "$PORT" -j DROP 2>/dev/null || true
    sudo iptables -D OUTPUT -p tcp --dport "$PORT" -j DROP 2>/dev/null || true
    echo "✅  Porta $PORT desbloqueada"
    ;;
  *)
    echo "❌  Ação inválida: $ACTION (use 'block' ou 'unblock')"
    exit 1
    ;;
esac

