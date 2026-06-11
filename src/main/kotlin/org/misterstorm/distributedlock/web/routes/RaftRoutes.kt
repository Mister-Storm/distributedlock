package org.misterstorm.distributedlock.web.routes

import kotlinx.coroutines.runBlocking
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.models.raft.ExcludeVoteInput
import org.misterstorm.distributedlock.core.models.raft.GossipData
import org.misterstorm.distributedlock.core.models.raft.HeartbeatInput
import org.misterstorm.distributedlock.core.models.raft.JoinInput
import org.misterstorm.distributedlock.core.models.raft.VoteInput
import org.misterstorm.distributedlock.core.usecases.raft.AcceptReplicaUseCase
import org.misterstorm.distributedlock.core.usecases.raft.CommitReplicaUseCase
import org.misterstorm.distributedlock.core.usecases.raft.GetNodeStatusUseCase
import org.misterstorm.distributedlock.core.usecases.raft.GetSnapshotUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessExcludeVoteUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessGossipUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessHeartbeatUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessJoinUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessVoteUseCase
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

@RestController
class RaftRoutes(
    private val processHeartbeatUseCase: ProcessHeartbeatUseCase,
    private val processVoteUseCase: ProcessVoteUseCase,
    private val processGossipUseCase: ProcessGossipUseCase,
    private val processJoinUseCase: ProcessJoinUseCase,
    private val processExcludeVoteUseCase: ProcessExcludeVoteUseCase,
    private val acceptReplicaUseCase: AcceptReplicaUseCase,
    private val commitReplicaUseCase: CommitReplicaUseCase,
    private val getNodeStatusUseCase: GetNodeStatusUseCase,
    private val getSnapshotUseCase: GetSnapshotUseCase,
) : RaftRoutesSpec {

    override fun heartbeat(request: HeartbeatRequest): ResponseEntity<*> {
        val input = HeartbeatInput(
            leaderName = request.leaderName,
            leaderUrl = request.leaderUrl,
            term = request.term,
            recentCommits = request.recentCommits,
        )
        return runBlocking { processHeartbeatUseCase.execute(input) }.fold(
            { ResponseEntity.badRequest().body("Stale term or already leader") },
            { ResponseEntity.ok().build<Unit>() }
        )
    }

    override fun vote(request: VoteRequest): ResponseEntity<VoteResponse> {
        val input = VoteInput(
            candidateName = request.candidateName,
            candidateUrl = request.candidateUrl,
            term = request.term,
        )
        val result = runBlocking { processVoteUseCase.execute(input) }
        return result.fold(
            { ResponseEntity.badRequest().build() },
            { output -> ResponseEntity.ok(VoteResponse(term = output.term, voteGranted = output.voteGranted)) }
        )
    }

    override fun gossip(message: GossipMessage): ResponseEntity<GossipMessage> {
        val result = runBlocking {
            processGossipUseCase.execute(GossipData(nodes = message.nodes))
        }
        return ResponseEntity.ok(GossipMessage(nodes = result.nodes))
    }

    override fun replicate(request: ReplicateRequest): ResponseEntity<*> {
        runBlocking {
            acceptReplicaUseCase.execute(
                ReplicaEntry(
                    idempotencyKey = request.idempotencyKey,
                    operation = request.operation,
                    lock = request.lock,
                )
            )
        }
        return ResponseEntity.ok().build<Unit>()
    }

    override fun commit(request: CommitRequest): ResponseEntity<*> {
        runBlocking { commitReplicaUseCase.execute(request.idempotencyKey) }
        return ResponseEntity.ok().build<Unit>()
    }

    override fun status(): ResponseEntity<*> {
        val status = runBlocking { getNodeStatusUseCase.execute(Unit) }
        return ResponseEntity.ok(
            mapOf(
                "node" to status.node,
                "url" to status.url,
                "role" to status.role,
                "term" to status.term,
                "leader" to status.leader,
                "leaderUrl" to status.leaderUrl,
                "peers" to status.peers,
                "knownNodes" to status.knownNodes,
                "healthy_peers" to status.healthyPeers,
                "unhealthy_peers" to status.unhealthyPeers,
                "healthy_cluster_size" to status.healthyClusterSize,
                "peer_details" to status.peerDetails.map { peer ->
                    mapOf(
                        "name" to peer.name,
                        "url" to peer.url,
                        "status" to peer.reachability.name,
                        "consecutive_failures" to peer.consecutiveFailures,
                        "last_seen_at" to peer.lastSeenAt?.toString(),
                        "last_health_check_at" to peer.lastHealthCheckAt?.toString(),
                    )
                },
                "cluster_health" to mapOf(
                    "healthy_cluster_size" to status.healthyClusterSize,
                    "healthy_peers" to status.healthyPeers,
                    "unhealthy_peers" to status.unhealthyPeers,
                ),
                "locks" to status.locks,
                "locks_in_queue" to status.locksInQueue,
                "chaos" to status.chaos,
            )
        )
    }

    override fun snapshot(): ResponseEntity<*> =
        runBlocking { getSnapshotUseCase.execute(Unit) }.fold(
            { ResponseEntity.status(403).body("Not the leader") },
            { data ->
                @Suppress("UNCHECKED_CAST")
                ResponseEntity.ok(
                    SnapshotResponse(
                        locks = data.locks as Collection<org.misterstorm.distributedlock.core.models.lock.Lock>,
                        queue = data.queue as Collection<org.misterstorm.distributedlock.core.models.lock.Lock>,
                    )
                )
            }
        )

    override fun join(request: JoinRequest): ResponseEntity<GossipMessage> {
        val result = runBlocking { processJoinUseCase.execute(JoinInput(name = request.name, url = request.url)) }
        return ResponseEntity.ok(GossipMessage(nodes = result.nodes))
    }

    override fun excludeVote(request: ExcludeVoteRequest): ResponseEntity<ExcludeVoteResponse> {
        val result = runBlocking { processExcludeVoteUseCase.execute(ExcludeVoteInput(suspectUrl = request.suspectUrl)) }
        return ResponseEntity.ok(ExcludeVoteResponse(exclude = result.exclude))
    }
}