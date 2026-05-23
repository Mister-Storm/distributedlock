package org.misterstorm.distributedlock.infra.raft

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*

@Service
class RaftReplicationService(
    private val nodeRegistry: NodeRegistry,
    private val heartbeatService: HeartbeatService,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
) {

    fun replicate(operation: LockOperation, lock: Lock): Boolean {
        val idempotencyKey = UUID.randomUUID().toString()
        val peers = nodeRegistry.getPeerUrls()
        if (peers.isEmpty()) return true

        val replicateBody = objectMapper.writeValueAsString(
            ReplicateRequest(idempotencyKey, operation, lock)
        )
        val acks = peers.count { peer ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$peer/raft/replicate"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(replicateBody))
                    .timeout(Duration.ofSeconds(3))
                    .build()
                val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                response.statusCode() == 200
            }.getOrDefault(false)
        }

        val clusterSize = peers.size + 1
        val quorum = clusterSize / 2 + 1
        val totalAcks = acks + 1
        if (totalAcks < quorum) {
            println("Replication failed: got $totalAcks ACKs, needed $quorum (cluster=$clusterSize)")
            return false
        }

        heartbeatService.recordCommit(idempotencyKey)

        val commitBody = objectMapper.writeValueAsString(CommitRequest(idempotencyKey))
        peers.forEach { peerUrl ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$peerUrl/raft/commit"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(commitBody))
                    .timeout(Duration.ofSeconds(2))
                    .build()
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            }
        }
        return true
    }
}
data class ReplicateRequest(
    val idempotencyKey: String,
    val operation: LockOperation,
    val lock: Lock,
)

data class CommitRequest(val idempotencyKey: String)