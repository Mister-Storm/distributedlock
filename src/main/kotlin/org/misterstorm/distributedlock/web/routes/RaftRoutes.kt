package org.misterstorm.distributedlock.web.routes

import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteResponse
import org.misterstorm.distributedlock.infra.raft.services.CommitRequest
import org.misterstorm.distributedlock.infra.raft.services.ExcludeVoteRequest
import org.misterstorm.distributedlock.infra.raft.services.ExcludeVoteResponse
import org.misterstorm.distributedlock.infra.raft.services.GossipMessage
import org.misterstorm.distributedlock.infra.raft.services.JoinRequest
import org.misterstorm.distributedlock.infra.raft.services.ReplicateRequest
import org.misterstorm.distributedlock.infra.raft.services.SnapshotResponse
import org.misterstorm.distributedlock.web.routes.spec.RaftRoutesSpec
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@RestController
class RaftRoutes(
    private val nodeState: NodeState,
    private val nodeRegistry: NodeRegistry,
    private val lockRepository: LockRepository,
    private val httpClient: HttpClient,
) : RaftRoutesSpec {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun heartbeat(request: HeartbeatRequest): ResponseEntity<*> {
        MDC.put("leaderName", request.leaderName)
        MDC.put("term", request.term.toString())
        if (request.term < nodeState.currentTerm.get() || nodeState.isLeader()) {
            log.warn("Heartbeat rejected: stale term or node is already leader")
            MDC.remove("leaderName"); MDC.remove("term")
            return ResponseEntity.badRequest().body("Stale term")
        }
        nodeState.becomeFollower(request.term, request.leaderName, request.leaderUrl)
        request.recentCommits.forEach { key ->
            if (lockRepository.hasPending(key)) {
                lockRepository.commit(key)
            }
        }
        log.info("Heartbeat accepted")
        MDC.remove("leaderName"); MDC.remove("term")
        return ResponseEntity.ok().build<Unit>()
    }

    override fun vote(request: VoteRequest): ResponseEntity<VoteResponse> {
        val currentTerm = nodeState.currentTerm.get()
        MDC.put("candidate", request.candidateName)
        MDC.put("requestTerm", request.term.toString())
        MDC.put("currentTerm", currentTerm.toString())
        if (request.term < currentTerm) {
            log.warn("Vote denied: candidate term is stale")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            return ResponseEntity.ok(VoteResponse(currentTerm, false))
        }
        val alreadyVoted = nodeState.votedFor.get()
        val canVote = alreadyVoted == null || alreadyVoted == request.candidateName
        return if (canVote) {
            val vote = if (nodeState.nodeName > request.candidateName) nodeState.nodeName else request.candidateName
            nodeState.voteFor(vote)
            nodeRegistry.merge(mapOf(request.candidateName to request.candidateUrl))
            log.info("Vote granted")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            ResponseEntity.ok(VoteResponse(request.term, true))
        } else {
            log.info("Vote denied: already voted in this term")
            nodeState.clearVotedFor()
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            ResponseEntity.ok(VoteResponse(currentTerm, false))
        }
    }

    override fun gossip(message: GossipMessage): ResponseEntity<GossipMessage> {
        nodeRegistry.merge(message.nodes)
        return ResponseEntity.ok(GossipMessage(nodeRegistry.getAllNodes()))
    }

    override fun replicate(request: ReplicateRequest): ResponseEntity<*> {
        MDC.put("idempotencyKey", request.idempotencyKey)
        MDC.put("operation", request.operation.name)
        MDC.put("lockKey", request.lock.key)
        if (lockRepository.hasPending(request.idempotencyKey)) {
            log.info("Replicate request already pending (idempotent)")
            MDC.remove("idempotencyKey"); MDC.remove("operation"); MDC.remove("lockKey")
            return ResponseEntity.ok().build<Unit>()
        }
        lockRepository.savePending(ReplicaEntry(request.idempotencyKey, request.operation, request.lock))
        log.info("Replicate request accepted and saved as pending")
        MDC.remove("idempotencyKey"); MDC.remove("operation"); MDC.remove("lockKey")
        return ResponseEntity.ok().build<Unit>()
    }

    override fun commit(request: CommitRequest): ResponseEntity<*> {
        MDC.put("idempotencyKey", request.idempotencyKey)
        lockRepository.commit(request.idempotencyKey)
        log.info("Commit applied")
        MDC.remove("idempotencyKey")
        return ResponseEntity.ok().build<Unit>()
    }

    override fun status(): ResponseEntity<*> = ResponseEntity.ok(
        mapOf(
            "node" to nodeState.nodeName,
            "url" to nodeState.nodeUrl,
            "role" to nodeState.role.get(),
            "term" to nodeState.currentTerm.get(),
            "leader" to nodeState.leaderId.get(),
            "leaderUrl" to nodeState.leaderUrl.get(),
            "peers" to nodeRegistry.getPeerUrls(),
            "knownNodes" to nodeRegistry.getAllNodes(),
            "locks" to lockRepository.getAllLocks(),
            "locks_in_queue" to lockRepository.getAllInQueue(),
        )
    )

    override fun snapshot(): ResponseEntity<*> {
        MDC.put("node", nodeState.nodeName)
        if (!nodeState.isLeader()) {
            log.warn("Snapshot request rejected: node is not the leader")
            MDC.remove("node")
            return ResponseEntity.status(403).body("Not the leader")
        }
        log.info("Snapshot requested by follower")
        MDC.remove("node")
        return ResponseEntity.ok(
            SnapshotResponse(
                locks = lockRepository.getAllLocks().filter { !it.isExpired() },
                queue = lockRepository.getAllInQueue(),
            )
        )
    }

    override fun join(request: JoinRequest): ResponseEntity<GossipMessage> {
        MDC.put("joiningNode", request.name)
        MDC.put("joiningUrl", request.url)
        nodeRegistry.merge(mapOf(request.name to request.url))
        log.info("Node joined the cluster")
        MDC.remove("joiningNode"); MDC.remove("joiningUrl")
        return ResponseEntity.ok(GossipMessage(nodeRegistry.getAllNodes()))
    }

    override fun excludeVote(request: ExcludeVoteRequest): ResponseEntity<ExcludeVoteResponse> {
        MDC.put("suspectUrl", request.suspectUrl)
        val canReach = runCatching {
            val response = httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("${request.suspectUrl}/raft/status"))
                    .GET()
                    .timeout(Duration.ofSeconds(2))
                    .build(),
                HttpResponse.BodyHandlers.discarding()
            )
            response.statusCode() == 200
        }.getOrDefault(false)
        MDC.put("canReach", canReach.toString())
        log.info("Exclude vote cast")
        MDC.remove("suspectUrl"); MDC.remove("canReach")
        return ResponseEntity.ok(ExcludeVoteResponse(exclude = !canReach))
    }
}