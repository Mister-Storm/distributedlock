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
): RaftRoutesSpec {
    override fun heartbeat(request: HeartbeatRequest): ResponseEntity<*> {
        if (request.term < nodeState.currentTerm.get() || nodeState.isLeader()) {
            return ResponseEntity.badRequest().body("Stale term")
        }
        nodeState.becomeFollower(request.term, request.leaderName, request.leaderUrl)
        //TODO move this to another endpoint
        request.recentCommits.forEach { key ->
            if (lockRepository.hasPending(key)) {
                lockRepository.commit(key)
            }
        }
        return ResponseEntity.ok().build<Unit>()
    }

    override fun vote(request: VoteRequest): ResponseEntity<VoteResponse> {
        val currentTerm = nodeState.currentTerm.get()
        if (request.term < currentTerm) {
            return ResponseEntity.ok(VoteResponse(currentTerm, false))
        }
        val alreadyVoted = nodeState.votedFor.get()
        val canVote = alreadyVoted == null || alreadyVoted == request.candidateName
        return if (canVote) {
            val vote = if(nodeState.nodeName > request.candidateName) nodeState.nodeName else request.candidateName
            nodeState.voteFor(vote)
            nodeRegistry.merge(mapOf(request.candidateName to request.candidateUrl))
            ResponseEntity.ok(VoteResponse(request.term, true))
        } else {
            nodeState.clearVotedFor()
            ResponseEntity.ok(VoteResponse(currentTerm, false))
        }
    }

    override fun gossip(message: GossipMessage): ResponseEntity<GossipMessage> {
        nodeRegistry.merge(message.nodes)
        return ResponseEntity.ok(GossipMessage(nodeRegistry.getAllNodes()))
    }

    override fun replicate(request: ReplicateRequest): ResponseEntity<*> {
        if (lockRepository.hasPending(request.idempotencyKey)) {
            return ResponseEntity.ok().build<Unit>()
        }
        lockRepository.savePending(
            ReplicaEntry(request.idempotencyKey, request.operation, request.lock)
        )
        return ResponseEntity.ok().build<Unit>()
    }

    override fun commit(request: CommitRequest): ResponseEntity<*> {
        lockRepository.commit(request.idempotencyKey)
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
        if (!nodeState.isLeader()) {
            return ResponseEntity.status(403).body("Not the leader")
        }
        return ResponseEntity.ok(
            SnapshotResponse(
                locks = lockRepository.getAllLocks().filter { !it.isExpired() },
                queue = lockRepository.getAllInQueue(),
            )
        )
    }

    override fun join(request: JoinRequest): ResponseEntity<GossipMessage> {
        nodeRegistry.merge(mapOf(request.name to request.url))
        return ResponseEntity.ok(GossipMessage(nodeRegistry.getAllNodes()))
    }

    override fun excludeVote(request: ExcludeVoteRequest): ResponseEntity<ExcludeVoteResponse> {
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
        return ResponseEntity.ok(ExcludeVoteResponse(exclude = !canReach))
    }
}