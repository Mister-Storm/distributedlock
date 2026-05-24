# DistributedLock — Observability Stack

## Modos de operação

| Modo | Arquivo | Quando usar |
|---|---|---|
| **Tudo no Docker** | `docker-compose.yml` | CI, testes integrados, demo simples |
| **Só observabilidade** | `docker-compose.observability.yml` | Demo de falhas, partição de rede, kill de nó |

---

## Modo Tudo no Docker

```bash
docker compose up --build -d
```

---

## Modo Observabilidade Separada (recomendado para demos)

### Arquitetura

```
Processo local (JVM)                   Docker
────────────────────                   ──────────────────────────
node1  → logs/node1.log ──┐
node2  → logs/node2.log ──┤── volume ./logs:/logs:ro ──► Promtail ──► Loki ──► Grafana :3000
node3  → logs/node3.log ──┘
nodeN  → logs/nodeN.log      (detectado automaticamente quando o arquivo aparece)
```

**Vantagem:** os nós são processos comuns — fácil de matar (`stop-node.sh`), simular crash (`--kill`) ou partição de rede (`network-partition.sh`). O Grafana continua mostrando os logs em tempo real independente dos nós.

### 1. Build

```bash
./gradlew bootJar
```

### 2. Subir observabilidade

```bash
docker compose -f docker-compose.observability.yml up -d
# Grafana: http://localhost:3000
```

### 3. Iniciar nós localmente

```bash
./scripts/start-node.sh node1 8081 "http://localhost:8082,http://localhost:8083"
./scripts/start-node.sh node2 8082 "http://localhost:8081,http://localhost:8083"
./scripts/start-node.sh node3 8083 "http://localhost:8081,http://localhost:8082"
```

Os logs aparecem em `logs/node1.log`, `logs/node2.log`, `logs/node3.log`  
O Promtail detecta cada arquivo novo automaticamente.

### 4. Ver status dos nós

```bash
./scripts/status.sh
```

---

## Cenários de Demo

### Cenário 1: Queda de nó (crash)

```bash
# Para node2 abruptamente (SIGKILL — sem graceful shutdown)
./scripts/stop-node.sh node2 --kill

# No Grafana: filtrar service=node2 — logs param
#             filtrar service=node1 ou node3 — ver eleição disparada
#             painel "Election Events" mostra a nova eleição
```

### Cenário 2: Queda graceful (SIGTERM)

```bash
./scripts/stop-node.sh node2

# No Grafana: ver "Node transitioned to CANDIDATE" e "Election won" nos demais nós
```

### Cenário 3: Partição de rede (iptables)

```bash
# Isola node2 (porta 8082) da rede — demais nós param de recebê-lo
sudo ./scripts/network-partition.sh block 8082

# No Grafana: ver heartbeat failures, eleição, novo leader
# Restaurar:
sudo ./scripts/network-partition.sh unblock 8082
# Ver: node2 volta como FOLLOWER, sincroniza estado
```

### Cenário 4: Adicionar nó novo dinamicamente

```bash
# Inicia um 4º nó — aparece no Grafana em segundos sem restart do Promtail
./scripts/start-node.sh node4 8084 "http://localhost:8081,http://localhost:8082,http://localhost:8083"
```

### Cenário 5: Parar tudo

```bash
for node in node1 node2 node3; do ./scripts/stop-node.sh $node; done
docker compose -f docker-compose.observability.yml down -v
```

---

## Grafana — Dashboard

Acesse: **http://localhost:3000** → pasta **DistributedLock**

| Painel | O que mostra |
|---|---|
| **All Logs** | Stream completo, filtrável por nó e nível |
| **Warnings & Errors** | `WARN` e `ERROR` de todos os nós |
| **Raft State Transitions** | LEADER / CANDIDATE / FOLLOWER |
| **Log Rate by Node** | Gráfico de taxa de logs por nó |
| **Error Rate by Node** | Taxa de `WARN` e `ERROR` por nó |
| **Lock Operations** | POST/DELETE/PUT de locks com `traceId` |
| **Replication Events** | Quórum, replicação, commits |
| **Election Events** | Eleições, votos, resultado |

---

## Como a descoberta automática funciona

### Docker mode (docker-compose.yml)
Promtail usa **Docker Service Discovery** via `/var/run/docker.sock`.  
Qualquer container com label `logging=promtail` é detectado em até 10s.

### Local mode (docker-compose.observability.yml)
Promtail usa **static_configs** com glob `__path__: /logs/*.log`.  
Qualquer arquivo `logs/nodeX.log` novo é detectado via inotify (segundos).  
O label `service` é extraído do nome do arquivo: `node42.log → service=node42`.

---

## Labels disponíveis para filtros no Grafana

| Label (indexado) | Valores exemplo |
|---|---|
| `service` | `node1`, `node2`, `node3` |
| `level` | `INFO`, `WARN`, `ERROR` |
| `raft_node` | nome do nó (MDC `node`) |
| `operation` | `POST /lock`, `DELETE /lock`, `PUT /lock/renew` |

| Structured Metadata (não indexado) | Descrição |
|---|---|
| `trace_id` | UUID por requisição HTTP |
| `lock_key` | Chave do distributed lock |
| `client_id` | ID do cliente |
| `raft_term` | Term Raft corrente |
| `peer` | URL do peer |
| `idempotency_key` | Chave de idempotência da replicação |
| `error_type` | Tipo do erro de negócio |
