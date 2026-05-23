package org.misterstorm.distributedlock.web.routes

import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.*
import org.misterstorm.distributedlock.web.routes.spec.RaftRoutesSpec
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RestController

@RestController
class RaftRoutes(
    private val nodeState: NodeState,
    private val nodeRegistry: NodeRegistry,
    private val lockRepository: LockRepository,
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
        )
    )

}