# Distributed Lock Service

> Trabalho Final — Computação Distribuída  
> Mestrado em Ciência da Computação

Implementação de um serviço de **lock distribuído com consenso Raft**, desenvolvido em Kotlin com Spring Boot. O sistema garante **exclusão mútua** em ambientes distribuídos, tolerando falhas de nós e partições de rede, por meio de um protocolo de eleição de líder, replicação de log e mecanismo de fila com justiça entre clientes.

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura](#2-arquitetura)
3. [Protocolo Raft](#3-protocolo-raft)
4. [Gerenciamento de Locks](#4-gerenciamento-de-locks)
5. [API REST](#5-api-rest)
6. [Modelo de Erros](#6-modelo-de-erros)
7. [Configuração](#7-configuração)
8. [Como Executar](#8-como-executar)
9. [Testes](#9-testes)
10. [Observabilidade](#10-observabilidade)
11. [Decisões de Design](#11-decisões-de-design)
12. [Stack Tecnológica](#12-stack-tecnológica)

---

## 1. Visão Geral

Um **lock distribuído** é um mecanismo de controle de concorrência que garante que apenas um cliente de um sistema distribuído possa acessar um recurso compartilhado em um dado momento. Diferentemente de um mutex convencional (que opera em memória compartilhada dentro de um processo), o lock distribuído precisa ser coordenado entre múltiplos processos, possivelmente em máquinas diferentes, e sobreviver a falhas parciais do sistema.

### Propriedades Garantidas

| Propriedade | Descrição |
|---|---|
| **Exclusão mútua** | No máximo um cliente detém o lock de um recurso em qualquer instante |
| **Tolerância a falhas** | O sistema continua operando enquanto a maioria dos nós (quorum) estiver ativa |
| **Consistência** | Todas as escritas são replicadas com confirmação de quorum antes de serem confirmadas |
| **Fairness** | Clientes que aguardam um lock são atendidos em ordem de chegada via fila |
| **TTL automático** | Locks possuem tempo de expiração, evitando deadlocks por falha de cliente |
| **Redirecionamento** | Requisições a não-líderes são automaticamente redirecionadas (HTTP 307) |

### Modelo de Falha Tolerado

O sistema segue o modelo de falha **crash-stop**: um nó para de responder completamente (não produz respostas incorretas). É tolerado até **f = ⌊(n-1)/2⌋** falhas simultâneas, onde `n` é o tamanho do cluster.

| Cluster | Quorum | Falhas toleradas |
|:---:|:---:|:---:|
| 3 nós | 2 | 1 |
| 5 nós | 3 | 2 |
| 7 nós | 4 | 3 |

---

## 2. Arquitetura

O projeto adota **arquitetura hexagonal** (Ports & Adapters), organizando o código em três camadas concêntricas:

```
┌──────────────────────────────────────────────────────────┐
│                        web/                               │
│            Controllers (LockRoutes, RaftRoutes)           │
│  ┌────────────────────────────────────────────────────┐  │
│  │                     infra/                          │  │
│  │  Spring Beans, HTTP Client, Repository InMemory     │  │
│  │  ┌──────────────────────────────────────────────┐  │  │
│  │  │                  core/                        │  │  │
│  │  │   Domain Models · Use Cases · Adapter Ports   │  │  │
│  │  │   Zero dependências de framework              │  │  │
│  │  └──────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Estrutura de Pacotes

```
src/main/kotlin/org/misterstorm/distributedlock/
│
├── core/                          # Domínio puro — sem anotações Spring
│   ├── models/
│   │   ├── lock/                  # Lock, LockCandidate, LockOperation, ReplicaEntry
│   │   └── raft/                  # NodeInfo, RaftModels (VoteInput/Output, HeartbeatInput...)
│   ├── usecases/
│   │   ├── lock/                  # CreateLock, Release, Renew, GetStatus
│   │   └── raft/                  # ProcessHeartbeat, ProcessVote, StartElection,
│   │                              # AcceptReplica, CommitReplica, GetSnapshot,
│   │                              # ProcessGossip, ProcessJoin, ProcessExcludeVote
│   ├── adapter/                   # Interfaces de porta (LockRepository, ReplicationService,
│   │                              # LeaderStatus, NodeStateRepository, PeerRepository,
│   │                              # VoteRequester, NodeReachabilityChecker, CommitTracker)
│   ├── errors/                    # sealed interface BusinessError
│   ├── async/                     # interface Publisher<T>
│   └── support/                   # verifyLeadership(), verifyQuorum() — funções puras
│
├── infra/                         # Adaptadores de infraestrutura
│   ├── raft/
│   │   ├── adapters/              # Implementações: VoteRequesterHttp, NodeReachabilityCheckerHttp
│   │   ├── models/                # NodeRegistry (mapa nome→url dos peers)
│   │   ├── repository/            # NodeStateRepositoryInMemory, CommitTrackerInMemory,
│   │   │                          # PeerRepositoryInMemory
│   │   ├── requests/              # DTOs de entrada (HeartbeatRequest, VoteRequest...)
│   │   └── services/              # ElectionService, HeartbeatService, GossipService,
│   │                              # NodeJoinService, RaftReplicationService,
│   │                              # ExpiredLockCleanupService, NodesManagementService
│   ├── repository/                # LockRepositoryInMemory
│   └── assync/                    # FailLockPublisher, QueueSubscriber (Spring Events)
│
├── web/
│   └── routes/                    # LockRoutes, RaftRoutes (implementam *Spec interfaces)
│       ├── spec/                  # LockRoutesSpec, RaftRoutesSpec (contratos com anotações)
│       └── responses/             # ErrorResponse
│
└── config/                        # UseCaseConfig — fiação manual dos use cases como @Bean
```

### Princípio de Inversão de Dependência

Os use cases do `core` **nunca importam** classes de `infra` ou `web`. A fiação é feita em `config/UseCaseConfig.kt`, onde Spring injeta as implementações concretas via interfaces de porta:

```
core/usecases/CreateLockUseCase
        │ usa
        ▼
core/adapter/ReplicationService  ◄── implementado por ──  infra/raft/services/RaftReplicationService
core/adapter/LeaderStatus        ◄── implementado por ──  infra/raft/repository/NodeStateRepositoryInMemory
core/repository/LockRepository   ◄── implementado por ──  infra/repository/LockRepositoryInMemory
```

---

## 3. Protocolo Raft

### Visão Geral do Consenso

Raft é um algoritmo de consenso que garante que um conjunto de nós concorde sobre uma sequência de operações. O sistema é dividido em três subproblemas: **eleição de líder**, **replicação de log** e **segurança**.

Referência: Ongaro, D. e Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*.

### Papéis dos Nós

```
                    timeout sem heartbeat
FOLLOWER  ─────────────────────────────────►  CANDIDATE
   ▲                                              │
   │                                    votos ≥ quorum
   │ recebe heartbeat                             │
   │ de um líder válido                           ▼
   └──────────────────────────────────────  LEADER
```

| Papel | Comportamento |
|---|---|
| **FOLLOWER** | Aceita heartbeats e commits do líder; responde a votes; redireciona writes |
| **CANDIDATE** | Solicita votos; se obtiver quorum, torna-se líder |
| **LEADER** | Envia heartbeats periódicos; processa todas as escritas; replica operações |

### Eleição de Líder

**Trigger**: um follower não recebe heartbeat por `electionTimeout` milissegundos (padrão: 6.000 ms).

```
[ElectionService]
@Scheduled(fixedRate = electionTimeout)
  │
  ├─ isHeartbeatExpired()? → NÃO → fim
  │
  └─ SIM → [StartElectionUseCase]
              │
              ├─ 1. Transição para CANDIDATE: term++, vota em si mesmo
              ├─ 2. Envia POST /raft/vote para todos os peers (paralelo)
              ├─ 3. Cada peer executa ProcessVoteUseCase:
              │       - term do candidato < term atual → NEGA
              │       - já votou em outro neste term  → NEGA + clearVotedFor
              │       - sem voto ou já votou neste    → CONCEDE + registra peer
              ├─ 4. Contagem: votos_próprio(1) + votos_recebidos
              └─ 5. votos ≥ quorum → LEADER; caso contrário → mantém CANDIDATE
```

**Diagrama de sequência — Eleição:**

```
node1 (timeout)    node2              node3
      │                │                │
      ├─ POST /raft/vote (term=2) ──────►│
      │                ├─ voteGranted=true
      │◄───────────────┤                │
      ├─ POST /raft/vote (term=2) ───────────────►│
      │                │         voteGranted=true │
      │◄───────────────────────────────────────────┤
      │  votos=3 ≥ quorum(2) → LEADER
      │
      ├─ POST /raft/heartbeat ──────────►│
      ├─ POST /raft/heartbeat ───────────────────►│
```

### Heartbeat e Manutenção de Liderança

O líder envia heartbeats a cada **1.000 ms** carregando:
- `leaderName`, `leaderUrl`, `term`
- `recentCommits`: lista de `idempotencyKeys` recentemente confirmados

```
[HeartbeatService] @Scheduled(fixedRate = 1000)
  │
  └─ POST /raft/heartbeat → [ProcessHeartbeatUseCase] em cada follower
          │
          ├─ term < currentTerm        → rejeita (StaleTerm)
          ├─ nó já é LEADER            → rejeita (AlreadyLeader)
          └─ válido → becomeFollower(term, leaderName, leaderUrl)
                    → aplica recentCommits pendentes
```

Falhas consecutivas de heartbeat:
- ≥ 3 falhas → peer é **removido** do `NodeRegistry` pelo `HeartbeatService`
- ≥ 5 falhas no `NodesManagementService` (health check) → inicia **processo de exclusão por quorum**

### Processo de Exclusão por Quorum

Quando um nó é suspeito de estar morto, o `NodesManagementService` coleta votos de exclusão antes de removê-lo:

```
[NodesManagementService] @Scheduled(fixedRate = 4000)
  │
  └─ para cada peer com failures > 5:
        ├─ POST /raft/exclude-vote {suspectUrl} → cada peer saudável
        │       [ProcessExcludeVoteUseCase]:
        │         NodeReachabilityChecker.canReach(suspectUrl)
        │         → false: vote.exclude = true
        │         → true:  vote.exclude = false
        └─ excludeVotes ≥ quorum → nodeRegistry.remove(suspectUrl)
```

Isso evita falsos positivos de exclusão por partição assimétrica de rede.

### Replicação de Operações (Log Replication)

Toda operação de mutação de lock segue o fluxo de dois turnos (two-phase commit simplificado):

```
LÍDER                                    FOLLOWERS
  │                                           │
  ├─ 1. Aplica operação localmente            │
  │      (lockRepository.create/release/renew)│
  │                                           │
  ├─ 2. POST /raft/replicate ────────────────►│
  │      {idempotencyKey, operation, lock}    │
  │      [AcceptReplicaUseCase]:              │
  │       savePending(entry) se não duplicado │
  │◄─ 200 OK ─────────────────────────────────┤
  │                                           │
  ├─ 3. Conta ACKs; verifica quorum           │
  │                                           │
  ├─ 4. Se quorum atingido:                   │
  │      recordCommit(idempotencyKey)         │
  │      POST /raft/commit ──────────────────►│
  │      {idempotencyKey}                     │
  │      [CommitReplicaUseCase]:              │
  │       lockRepository.commit(key)          │
  │       → executa CREATE/RELEASE/RENEW/ENQUEUE
  │                                           │
  └─ 5. Se quorum não atingido:               │
         rollback local + erro 503            │
```

**Idempotência**: `AcceptReplicaUseCase` verifica `hasPending(idempotencyKey)` antes de salvar, tornando `/raft/replicate` idempotente por natureza.

### Operações Replicadas

```kotlin
enum class LockOperation { CREATE, RELEASE, RENEW, ENQUEUE }
```

| Operação | Quando é gerada | Efeito no follower após commit |
|---|---|---|
| `CREATE` | Novo lock concedido | Adiciona lock ao store |
| `RELEASE` | Lock liberado | Remove lock do store |
| `RENEW` | TTL renovado | Atualiza lock no store |
| `ENQUEUE` | Candidato aguarda lock já ocupado | Adiciona candidato à fila do recurso |

### Sincronização de Estado (Snapshot)

Quando um nó se junta ao cluster ou reinicia, ele sincroniza todo o estado do líder:

```
[NodeJoinService] (ApplicationRunner)
  │
  ├─ 1. POST /raft/join {name, url} → seed nodes
  │       [ProcessJoinUseCase]: registra nó, retorna gossip atual
  │
  ├─ 2. GET /raft/status → peers → descobre líder atual
  │
  └─ 3. GET /raft/snapshot → líder
          [GetSnapshotUseCase]: retorna {locks[], queue[]}
          lockRepository.loadSnapshot(locks, queue)
```

### Gossip Protocol

Para descoberta de nós e propagação de exclusões, o sistema usa gossip periódico:

```
[GossipService] @Scheduled(fixedRate = gossipInterval)
  │
  └─ POST /raft/gossip {nodes: Map<name,url>, deadNodes: Set<url>}
          [ProcessGossipUseCase]:
           nodeRegistry.merge(nodes)
           nodeRegistry.applyRemovals(deadNodes)
           retorna estado local atualizado
```

---

## 4. Gerenciamento de Locks

### Modelo de Dados

```kotlin
data class Lock(
    val key: String,               // Identificador do recurso
    val lockOwner: String,         // clientId do detentor
    val expirationTime: LocalDateTime  // TTL absoluto
)

data class LockCandidate(
    val key: String,
    val clientId: String
)
```

### Fluxo de Criação de Lock

```
POST /lock {key, clientId}
  │
  [CreateLockUseCase]
  │
  ├─ verifyLeadership() → não é líder? → 307 Redirect para leaderUrl
  │
  ├─ verifyExistentLock(): lock existe e não expirou?
  │       └─ SIM → raise LockAlreadyExists
  │
  ├─ getLock():
  │   ├─ hasKeyInQueue(key)? SIM → dequeue() → create() com TTL renovado
  │   │                             (candidato da fila tem prioridade)
  │   └─ NÃO → lockRepository.create(Lock(key, clientId, now+TTL))
  │
  ├─ verifyQuorum(replicate(CREATE, lock)):
  │   ├─ quorum OK  → retorna lock (HTTP 201)
  │   └─ quorum NOK → release(lock) + addQueue(lock) → erro 503
  │
  └─ onLeft (qualquer erro):
      ├─ LockAlreadyExists → replicate(ENQUEUE, candidate) + failLockPublisher
      │                                                     → addQueue local (HTTP 202)
      └─ outros → failLockPublisher → addQueue local
```

**Mecanismo de fila (fairness)**: quando um lock já existe, o candidato é adicionado à fila (`Queue`) **com replicação via ENQUEUE**. A fila é persistida em todos os nós para garantir que, se o líder cair, o novo líder continue honrando a ordem de chegada.

### Fluxo de Liberação de Lock

```
DELETE /lock {key, clientId}
  │
  [LockReleaseUseCase]
  │
  ├─ verifyLeadership() → não é líder? → 307 Redirect
  │
  ├─ getByKey(key): lock não encontrado? → 404
  │
  ├─ lockOwner ≠ clientId? → 409 Conflict
  │
  ├─ lockRepository.release(lock)  [remove local]
  │
  ├─ verifyQuorum(replicate(RELEASE, lock)):
  │   ├─ quorum OK  → onRight: promoteFromQueue(key)
  │   │                   │
  │   │                   ├─ hasKeyInQueue(key)? → NÃO → fim (HTTP 204)
  │   │                   └─ SIM → dequeue() → create(promoted, TTL=now+expirationTime)
  │   │                              replicate(CREATE, promoted)
  │   │                              ├─ quorum OK  → lock concedido ao próximo (HTTP 204)
  │   │                              └─ quorum NOK → release(promoted) + addQueue(candidate)
  │   │                                              + replicate(ENQUEUE, candidate)
  │   └─ quorum NOK → create(lock) [rollback] → erro 503
```

### Fluxo de Renovação de Lock

```
PUT /lock {key, clientId}
  │
  [LockRenewUseCase]
  │
  ├─ verifyLeadership() → não é líder? → 307 Redirect
  ├─ getByKey(key): não encontrado? → 404
  ├─ lockOwner ≠ clientId? → 409
  ├─ lock.renew(expirationTime) → nova expirationTime = now + TTL
  └─ verifyQuorum(replicate(RENEW, renewedLock)) → HTTP 200
```

### Limpeza de Locks Expirados

O `ExpiredLockCleanupService` roda periodicamente **apenas no líder**:

```
[ExpiredLockCleanupService] @Scheduled(fixedRate = lockCleanupInterval)
  │
  ├─ isLeader()? → NÃO → fim
  │
  └─ getAllLocks().filter { isExpired() }.forEach:
        ├─ replicate(RELEASE, expiredLock) → quorum NOK → retry no próximo ciclo
        │
        └─ hasKeyInQueue(key)?
              ├─ NÃO → fim
              └─ SIM → dequeue() → replicate(CREATE, promoted)
                           └─ quorum NOK → addQueue(promoted) [retry]
```

### Diagrama de Estado do Lock

```
                    POST /lock (clientId=A)
                           │
         ┌─────────────────▼──────────────────┐
         │         lock não existe             │
         │   create(key, A, now+TTL)           │
         │   replicate(CREATE)                 │
         └─────────────────┬──────────────────┘
                           │ quorum OK
                    ┌──────▼──────┐
                    │  LOCKED (A) │◄────────────────────────────┐
                    └──────┬──────┘                             │
                           │                                    │
              ┌────────────┼────────────┐                       │
              │            │            │                        │
    POST /lock (B)    PUT /lock (A)  DELETE /lock (A)           │
         │            TTL renew      + fila vazia               │
         ▼                │                │                     │
   addQueue(B)      LOCKED (A)      ──────────             hasKeyInQueue?
   ENQUEUE rep.     TTL renovado      UNLOCKED                   │ SIM
         │                                                       │
         │                              DELETE /lock (A)         │
         └─────────────────────────────► + fila com B ──────────┘
                                         dequeue(B)
                                         create(B, now+TTL)
                                         replicate(CREATE)
                                         LOCKED (B)
```

---

## 5. API REST

### Endpoints de Lock

| Método | Path | Corpo | Descrição |
|---|---|---|---|
| `POST` | `/lock` | `{key, clientId}` | Solicita lock; 201 se concedido, 202 se enfileirado |
| `DELETE` | `/lock` | `{key, clientId}` | Libera lock; promove candidato da fila se houver |
| `PUT` | `/lock` | `{key, clientId}` | Renova TTL do lock |
| `GET` | `/lock/{key}` | — | Consulta estado do lock |

### Endpoints Raft (internos)

| Método | Path | Descrição |
|---|---|---|
| `POST` | `/raft/heartbeat` | Recebe heartbeat do líder |
| `POST` | `/raft/vote` | Recebe solicitação de voto |
| `POST` | `/raft/replicate` | Aceita entrada de replicação (fase 1) |
| `POST` | `/raft/commit` | Confirma entrada replicada (fase 2) |
| `POST` | `/raft/gossip` | Troca informação de membership |
| `POST` | `/raft/join` | Entrada de novo nó no cluster |
| `POST` | `/raft/exclude-vote` | Voto para exclusão de nó suspeito |
| `GET` | `/raft/snapshot` | Retorna estado completo (locks + fila) |
| `GET` | `/raft/status` | Retorna estado do nó (role, term, peers, locks) |

### Exemplos de Uso

**Solicitar lock:**
```bash
curl -X POST http://localhost:8080/lock \
  -H "Content-Type: application/json" \
  -d '{"key": "meu-recurso", "clientId": "cliente-A"}'

# HTTP 201 — lock concedido
{
  "key": "meu-recurso",
  "lockOwner": "cliente-A",
  "expirationTime": "2026-06-05T10:30:00"
}

# HTTP 202 — lock já existe; cliente enfileirado
{
  "message": "Lock with key meu-recurso already exists. Request has been put in the queue.",
  ...
}
```

**Liberar lock:**
```bash
curl -X DELETE http://localhost:8080/lock \
  -H "Content-Type: application/json" \
  -d '{"key": "meu-recurso", "clientId": "cliente-A"}'

# HTTP 204 — liberado (próximo da fila promovido automaticamente se houver)
```

**Verificar estado:**
```bash
curl http://localhost:8080/lock/meu-recurso

# HTTP 200
{
  "key": "meu-recurso",
  "lockOwner": "cliente-A",
  "expirationTime": "2026-06-05T10:30:00",
  "isExpired": false
}
```

**Status do nó:**
```bash
curl http://localhost:8080/raft/status

{
  "node": "node1",
  "url": "http://localhost:8080",
  "role": "LEADER",
  "term": 3,
  "leader": "node1",
  "leaderUrl": "http://localhost:8080",
  "peers": ["http://localhost:8081", "http://localhost:8082"],
  "knownNodes": { "node2": "http://localhost:8081", "node3": "http://localhost:8082" },
  "locks": [...],
  "locks_in_queue": [...]
}
```

---

## 6. Modelo de Erros

Todos os erros de domínio são representados por `sealed interface BusinessError`:

| Erro | HTTP | Situação |
|---|---|---|
| `LockAlreadyExists` | 202 Accepted | Lock ocupado; cliente enfileirado |
| `NotLeader` | 307 Redirect | Nó não é líder; `Location` aponta para líder |
| `QuorumNotReached` | 503 Service Unavailable | Menos da maioria dos nós respondeu |
| `LockNotFound` | 404 Not Found | Lock não existe para o recurso |
| `ApplicantReleaseIsNotOwner` | 409 Conflict | Quem tenta liberar não é o dono |
| `StaleTerm` | 400 Bad Request | Heartbeat/vote com term desatualizado |
| `AlreadyLeader` | 400 Bad Request | Heartbeat recebido por um líder |
| `UnexpectedException` | 500 Internal Server Error | Erro não previsto |

**Comportamento de redirecionamento**: quando um cliente envia uma escrita a um follower, o follower responde com HTTP 307 e o header `Location: http://<leaderUrl>/lock`. O cliente deve seguir o redirect.

---

## 7. Configuração

### `application.yaml`

```yaml
distributedlock:
  node:
    name: ${NODE_NAME:node1}          # Nome único do nó
    url: ${NODE_URL:http://localhost:8080}  # URL pública do nó (usada em redirects e Raft)
    electionTimeout: 6000             # ms sem heartbeat antes de eleição
    gossipInterval: 3000              # ms entre rodadas de gossip
  raft:
    seeds: ${RAFT_SEEDS:http://localhost:8081,http://localhost:8082}  # Peers iniciais
  expirationTime: 120                 # TTL padrão dos locks em segundos

server:
  port: ${SERVER_PORT:8080}
```

### Variáveis de Ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `NODE_NAME` | `node1` | Nome único do nó no cluster |
| `NODE_URL` | `http://localhost:8080` | URL pública acessível pelos peers |
| `SERVER_PORT` | `8080` | Porta HTTP do servidor |
| `RAFT_SEEDS` | `http://localhost:8081,http://localhost:8082` | URLs dos seeds para join inicial |

---

## 8. Como Executar

### Pré-requisitos

- Java 21+
- Docker + Docker Compose (para modo containerizado)
- Gradle (ou usar `./gradlew`)

### Modo 1 — Nó Único (desenvolvimento)

```bash
./gradlew bootRun
```

O nó inicia na porta 8080 sem peers; como não encontra seeds, realiza eleição e torna-se líder automaticamente.

### Modo 2 — Cluster Local (3 nós)

Execute cada comando em um terminal separado:

```bash
# Terminal 1 — node1
./scripts/start-node.sh node1 8081 "http://localhost:8082,http://localhost:8083"

# Terminal 2 — node2
./scripts/start-node.sh node2 8082 "http://localhost:8081,http://localhost:8083"

# Terminal 3 — node3
./scripts/start-node.sh node3 8083 "http://localhost:8081,http://localhost:8082"
```

Verificar status:
```bash
./scripts/status.sh
```

### Modo 3 — Docker Compose Completo

Sobe 3 nós + Nginx (proxy/balanceador) + Loki + Promtail + Grafana:

```bash
docker compose up --build -d
```

| Serviço | URL |
|---|---|
| Proxy (qualquer nó) | http://localhost:8080 |
| Grafana (logs) | http://localhost:3000 |
| Loki | http://localhost:3100 |

**Arquitetura Docker:**

```
                         ┌──────────────────┐
Cliente HTTP ──────────► │  Nginx :8080     │
                         │  (proxy/balancer)│
                         └──┬───┬───┬───────┘
                            │   │   │
              ┌─────────────┘   │   └────────────┐
              ▼                 ▼                 ▼
        ┌──────────┐     ┌──────────┐     ┌──────────┐
        │  node1   │◄───►│  node2   │◄───►│  node3   │
        │ :8080    │     │ :8080    │     │ :8080    │
        └────┬─────┘     └────┬─────┘     └────┬─────┘
             │                │                 │
             └────────────────┼─────────────────┘
                              │ logs (labels)
                              ▼
                       ┌──────────┐     ┌──────────┐
                       │ Promtail │────►│   Loki   │
                       └──────────┘     └────┬─────┘
                                             ▼
                                       ┌──────────┐
                                       │ Grafana  │
                                       │  :3000   │
                                       └──────────┘
```

### Modo 4 — Observabilidade Separada (para demos)

Build + observabilidade no Docker, nós rodando localmente (ideal para simular falhas):

```bash
# 1. Compilar
./gradlew bootJar

# 2. Subir apenas a stack de observabilidade
docker compose -f docker-compose.observability.yml up -d

# 3. Iniciar nós localmente (logs em ./logs/nodeX.log)
./scripts/start-node.sh node1 8081 "http://localhost:8082,http://localhost:8083"
./scripts/start-node.sh node2 8082 "http://localhost:8081,http://localhost:8083"
./scripts/start-node.sh node3 8083 "http://localhost:8081,http://localhost:8082"
```

### Scripts Utilitários

```bash
# Status de todos os nós em execução
./scripts/status.sh

# Parar nó graciosamente (SIGTERM)
./scripts/stop-node.sh node2

# Parar nó abruptamente (SIGKILL — simula crash)
./scripts/stop-node.sh node2 --kill

# Simular partição de rede (requer sudo, usa iptables)
sudo ./scripts/network-partition.sh block 8082    # isola porta 8082
sudo ./scripts/network-partition.sh unblock 8082  # restaura
```

---

## 9. Testes

### Estratégia de Testes

O projeto adota três níveis de teste, alinhados com a arquitetura hexagonal:

```
┌──────────────────────────────────────────────────────────────┐
│  Integração  (web/routes/*IntegrationTest)                   │
│  @SpringBootTest + @AutoConfigureMockMvc                     │
│  Testa o sistema completo: HTTP → UseCase → Repository       │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Unitário — Infra  (infra/raft/services/*)             │  │
│  │  MockK para dependências HTTP                          │  │
│  │  ┌──────────────────────────────────────────────────┐  │  │
│  │  │  Unitário — Core  (core/usecases/*)              │  │  │
│  │  │  TestLockRepository (fake) + MockK               │  │  │
│  │  │  Zero Spring — rápido e isolado                  │  │  │
│  │  └──────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Testes Unitários do Core

Localizados em `src/test/kotlin/.../core/usecases/`:

| Classe de Teste | Cenários cobertos |
|---|---|
| `CreateLockUseCaseTest` | Criação bem-sucedida, lock existente (fila), TTL expirado, quorum falho, ENQUEUE replicado, liderança |
| `LockReleaseUseCaseTest` | Liberação, promoção da fila, re-enqueue em falha, não-dono, quorum falho |
| `LockRenewUseCaseTest` | Renovação, não-dono, lock não encontrado |
| `GetResourceLockStatusUseCaseTest` | Status de lock existente e inexistente |
| `AcceptAndCommitReplicaUseCaseTest` | Idempotência do accept, commit de CREATE/RELEASE/RENEW/ENQUEUE |
| `ProcessHeartbeatUseCaseTest` | Term válido/stale, nó já líder, commits pendentes |
| `ProcessVoteUseCaseTest` | Voto concedido/negado, term stale, já votou |
| `StartElectionUseCaseTest` | Eleição ganha, quorum não atingido, peer inacessível |
| `GetSnapshotUseCaseTest` | Líder retorna snapshot, não-líder retorna erro |
| `ProcessGossipAndJoinUseCaseTest` | Merge de nodes, retorno do gossip |
| `ProcessExcludeVoteUseCaseTest` | Nó alcançável vs. inacessível |

**Padrão de uso — TestLockRepository:**

```kotlin
val lockRepository = spyk(object : TestLockRepository() {
    override fun getByKey(key: String): Lock = createLock()
    override fun release(lock: Lock): Boolean = true
    override fun hasKeyInQueue(key: String): Boolean = false
})
val raftReplicationService = mockk<RaftReplicationService>()
every { raftReplicationService.replicate(any(), any()) } returns true

val sut = LockReleaseUseCase(lockRepository, createNodeState(), raftReplicationService, 120L)
```

### Testes de Integração

Localizados em `src/test/kotlin/.../web/routes/`:

**Setup padrão:**
```kotlin
@SpringBootTest
@AutoConfigureMockMvc
class RaftRoutesIntegrationTest {
    @BeforeEach fun setup() {
        (lockRepository as LockRepositoryInMemory).clear()
        nodeState.becomeFollower(0L, null, null)
        nodeRegistry.getPeerUrls().forEach { nodeRegistry.remove(it) }
    }
}
```

| Classe de Teste | Escopo |
|---|---|
| `LockRoutesIntegrationTest` | Endpoints `/lock` com contexto Spring completo |
| `RaftRoutesIntegrationTest` | Endpoints `/raft/*`, eleição, replicação, gossip |
| `NodeJoinServiceIntegrationTest` | Fluxo de join com MockServer simulando peers |

**MockServer** (`mockserver-netty`) é usado nos testes de integração para simular respostas de peers externos sem subir nós reais.

### DSL de Testes

`TestDsl.kt` fornece um builder fluente para os testes HTTP:

```kotlin
mvc.http(objectMapper)
    .post("/lock")
    .withBody(lockCandidate(key = "resource-1", clientId = "client-A"))
    .expectStatus(201)
    .expectJsonPath("$.key", "resource-1")
    .execute()
```

### Executar Testes

```bash
# Todos os testes
./gradlew test

# Apenas unitários do core
./gradlew test --tests "org.misterstorm.distributedlock.core.*"

# Apenas integração
./gradlew test --tests "org.misterstorm.distributedlock.web.*"

# Relatório HTML
open build/reports/tests/test/index.html
```

---

## 10. Observabilidade

### Logs Estruturados

Todo log usa **Logback + Logstash Encoder** produzindo JSON estruturado, com campos MDC propagados como metadados:

| Campo MDC | Tipo | Descrição |
|---|---|---|
| `traceId` | UUID | Identifica uma requisição HTTP end-to-end |
| `clientId` | String | ID do cliente solicitante |
| `lockKey` | String | Chave do recurso |
| `operation` | String | Operação atual (`POST /lock`, etc.) |
| `node` | String | Nome do nó |
| `term` | Long | Term Raft corrente |
| `peer` | String | URL do peer em comunicação |
| `idempotencyKey` | UUID | Chave da entrada de replicação |
| `errorType` | String | Tipo do erro de negócio |

### Grafana Dashboards

Acesse: **http://localhost:3000** → pasta **DistributedLock**

| Painel | O que mostra |
|---|---|
| **All Logs** | Stream completo, filtrável por nó e nível |
| **Warnings & Errors** | `WARN` e `ERROR` de todos os nós |
| **Raft State Transitions** | Transições LEADER / CANDIDATE / FOLLOWER |
| **Log Rate by Node** | Volume de logs por nó ao longo do tempo |
| **Error Rate by Node** | Taxa de erros por nó |
| **Lock Operations** | Criações, liberações e renovações de lock |
| **Replication Events** | Quórum, replicação, commits |
| **Election Events** | Eleições iniciadas, votos, resultado |

### Cenários de Observabilidade

**Cenário 1 — Queda de nó (crash):**
```bash
./scripts/stop-node.sh node2 --kill
# Observar: heartbeat failures → eleição → novo líder
```

**Cenário 2 — Partição de rede:**
```bash
sudo ./scripts/network-partition.sh block 8082
# Observar: node2 isolado → elections nos demais → node2 rejoin ao restaurar
sudo ./scripts/network-partition.sh unblock 8082
```

**Cenário 3 — Adição dinâmica de nó:**
```bash
./scripts/start-node.sh node4 8084 "http://localhost:8081,http://localhost:8082,http://localhost:8083"
# Observar: join → snapshot sync → gossip merge
```

---

## 11. Decisões de Design

### 1. Por que Raft e não Paxos?

Raft foi escolhido pela compreensibilidade (objetivo explícito do paper original) e por separar claramente os subproblemas de eleição, replicação e segurança. Em Paxos, a distinção entre fase de prepare e accept é mais difusa.

### 2. Armazenamento In-Memory

O estado (locks, fila, nodes) é mantido em memória (`ConcurrentHashMap`, `CopyOnWriteArrayList`). Isso é intencional para o contexto acadêmico: o foco é no protocolo de consenso, não na persistência. Em produção, seria substituído por um store durável (RocksDB, PostgreSQL).

### 3. Use Cases sem Spring

Os use cases do `core` são classes Kotlin puras, instanciadas manualmente em `UseCaseConfig.kt`. Isso garante:
- **Testabilidade**: instanciados com `new` nos testes unitários, sem contexto Spring
- **Portabilidade**: o domínio não está acoplado a nenhum framework
- **Clareza**: dependências explícitas no construtor

### 4. `Either<BusinessError, T>` (Arrow-kt)

Erros de domínio são valores de retorno, não exceções. Isso força o tratamento explícito no ponto de chamada e torna os fluxos de erro visíveis no tipo. Exceções técnicas (I/O, runtime) são capturadas nos adaptadores com `runCatching`.

### 5. Replicação ENQUEUE

A adição de candidatos à fila de espera é replicada via o mesmo mecanismo de log Raft (`LockOperation.ENQUEUE`). Sem isso, se o líder cair enquanto há candidatos na fila, o novo líder perderia esses candidatos. O `ENQUEUE` garante que a fila seja consistente em todos os nós.

### 6. Promoção Imediata na Liberação

Quando um lock é liberado (`DELETE /lock`), o próximo candidato da fila é promovido **imediatamente** pelo mesmo use case, sem aguardar o ciclo do `ExpiredLockCleanupService` (que roda a cada 3s). Isso reduz a latência de concessão para clientes aguardando.

### 7. `java.net.http.HttpClient` em vez de WebClient/RestTemplate

A comunicação HTTP entre nós usa a API nativa do Java 11+ (`java.net.http`), sem dependência do Spring WebFlux ou do RestTemplate deprecated. Isso mantém a camada `infra` com dependências mínimas.

### 8. Exclusão por Quorum de Nós

Um nó não é removido do cluster unilateralmente. O `NodesManagementService` coleta votos de exclusão de outros peers antes de remover. Isso evita que uma partição assimétrica de rede cause remoção incorreta de um nó saudável.

---

## 12. Stack Tecnológica

| Componente | Tecnologia | Versão |
|---|---|---|
| Linguagem | Kotlin | 2.2.21 |
| Runtime | JVM (OpenJDK) | 21 |
| Framework Web | Spring Boot | 4.0.6 |
| Programação Funcional | Arrow-kt | 2.1.2 |
| Coroutines | Kotlinx Coroutines | (spring managed) |
| Serialização JSON | Jackson (Kotlin module) | (spring managed) |
| HTTP Client (inter-nó) | `java.net.http.HttpClient` | Java 21 nativo |
| Logs estruturados | Logback + Logstash Encoder | 8.0 |
| Testes | JUnit 5 + MockK + MockServer | 5.x / 1.13.12 / 5.15.0 |
| Container | Docker + Docker Compose | — |
| Proxy | Nginx Alpine | — |
| Coleta de Logs | Grafana Promtail | 3.4.2 |
| Agregação de Logs | Grafana Loki | 3.4.2 |
| Dashboards | Grafana | 12.0.0 |

---

## Referências

- Ongaro, D., Ousterhout, J. (2014). **In Search of an Understandable Consensus Algorithm (Extended Version)**. USENIX ATC.  
- Lamport, L. (1998). **The Part-Time Parliament**. ACM Transactions on Computer Systems.  
- Fischer, M. J., Lynch, N. A., Paterson, M. S. (1985). **Impossibility of Distributed Consensus with One Faulty Process**. Journal of the ACM. *(Teorema FLP)*  
- Ports & Adapters Architecture — Cockburn, A. (2005). **Hexagonal Architecture**.  
- *Arrow-kt documentation*: https://arrow-kt.io/  
- *Raft visualization*: https://raft.github.io/

