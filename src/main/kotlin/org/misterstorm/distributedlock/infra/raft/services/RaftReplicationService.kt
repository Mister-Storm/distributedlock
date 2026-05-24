package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.slf4j.LoggerFactory
import org.slf4j.MDC
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
    private val log = LoggerFactory.getLogger(javaClass)

    fun replicate(operation: LockOperation, lock: Lock): Boolean {
        val idempotencyKey = UUID.randomUUID().toString()
        val peers = nodeRegistry.getPeerUrls()

        MDC.put("operation", operation.name)
        MDC.put("lockKey", lock.key)
        MDC.put("idempotencyKey", idempotencyKey)

        if (peers.isEmpty()) {
            log.info("No peers to replicate to, operation accepted locally")
            MDC.remove("operation"); MDC.remove("lockKey"); MDC.remove("idempotencyKey")
            return true
        }

        MDC.put("peers", peers.size.toString())
        log.info("Starting replication")
        MDC.remove("peers")

        val replicateBody = objectMapper.writeValueAsString(ReplicateRequest(idempotencyKey, operation, lock))
        val acks = peers.count { peer ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$peer/raft/replicate"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(replicateBody))
                    .timeout(Duration.ofSeconds(3))
                    .build()
                val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                val acked = response.statusCode() == 200
                if (!acked) {
                    MDC.put("peer", peer)
                    MDC.put("statusCode", response.statusCode().toString())
                    log.warn("Replication rejected by peer")
                    MDC.remove("peer"); MDC.remove("statusCode")
                }
                acked
            }.getOrElse { ex ->
                MDC.put("peer", peer)
                MDC.put("error", ex.message)
                log.warn("Replication request to peer failed")
                MDC.remove("peer"); MDC.remove("error")
                false
            }
        }

        val clusterSize = peers.size + 1
        val quorum = clusterSize / 2 + 1
        val totalAcks = acks + 1

        MDC.put("acks", totalAcks.toString())
        MDC.put("quorum", quorum.toString())
        MDC.put("clusterSize", clusterSize.toString())

        if (totalAcks < quorum) {
            log.warn("Replication failed: quorum not reached")
            MDC.remove("operation"); MDC.remove("lockKey"); MDC.remove("idempotencyKey")
            MDC.remove("acks"); MDC.remove("quorum"); MDC.remove("clusterSize")
            return false
        }

        log.info("Replication succeeded, broadcasting commits")
        MDC.remove("acks"); MDC.remove("quorum"); MDC.remove("clusterSize")

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
            }.onFailure { ex ->
                MDC.put("peer", peerUrl)
                MDC.put("error", ex.message)
                log.warn("Failed to send commit to peer")
                MDC.remove("peer"); MDC.remove("error")
            }
        }

        MDC.remove("operation"); MDC.remove("lockKey"); MDC.remove("idempotencyKey")
        return true
    }
}

data class ReplicateRequest(val idempotencyKey: String, val operation: LockOperation, val lock: Lock)
data class CommitRequest(val idempotencyKey: String)
data class JoinRequest(val name: String, val url: String)
data class ExcludeVoteRequest(val suspectUrl: String)
data class ExcludeVoteResponse(val exclude: Boolean)
data class SnapshotResponse(val locks: Collection<Lock>, val queue: Collection<Lock>)
