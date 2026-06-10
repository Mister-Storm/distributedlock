package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.CommitTracker
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.adapter.ReplicationService
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.infra.chaos.ClusterHttpClient
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*

@Service
class RaftReplicationService(
    private val peerRepository: PeerRepository,
    private val commitTracker: CommitTracker,
    private val clusterHttpClient: ClusterHttpClient,
    private val objectMapper: ObjectMapper,
): ReplicationService {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun replicate(operation: LockOperation, lock: Lock): Boolean {
        val idempotencyKey = UUID.randomUUID().toString()
        val allPeers = peerRepository.getPeerUrls()
        val reachablePeers = peerRepository.getReachablePeerUrls()

        MDC.put("operation", operation.name)
        MDC.put("lockKey", lock.key)
        MDC.put("idempotencyKey", idempotencyKey)

        if (allPeers.isEmpty()) {
            log.info("No peers to replicate to, operation accepted locally")
            MDC.remove("operation"); MDC.remove("lockKey"); MDC.remove("idempotencyKey")
            return true
        }

        MDC.put("peers", reachablePeers.size.toString())
        log.info("Starting replication")
        MDC.remove("peers")

        val replicateBody = objectMapper.writeValueAsString(ReplicateRequest(idempotencyKey, operation, lock))
        val acks = reachablePeers.count { peer ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$peer/raft/replicate"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(replicateBody))
                    .timeout(Duration.ofSeconds(3))
                    .build()
                val response = clusterHttpClient.send(request, HttpResponse.BodyHandlers.ofString())
                val acked = response.statusCode() == 200
                if (acked) {
                    peerRepository.markReachable(peer)
                } else {
                    MDC.put("peer", peer)
                    MDC.put("statusCode", response.statusCode().toString())
                    log.warn("Replication rejected by peer")
                    peerRepository.markUnreachable(peer)
                    MDC.remove("peer"); MDC.remove("statusCode")
                }
                acked
            }.getOrElse { ex ->
                MDC.put("peer", peer)
                MDC.put("error", ex.message)
                log.warn("Replication request to peer failed")
                peerRepository.markUnreachable(peer)
                MDC.remove("peer"); MDC.remove("error")
                false
            }
        }

        val clusterSize = allPeers.size + 1
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

        commitTracker.recordCommit(idempotencyKey)

        val commitBody = objectMapper.writeValueAsString(CommitRequest(idempotencyKey))
        reachablePeers.forEach { peerUrl ->
            sendCommitWithRetry(peerUrl, commitBody)
        }

        MDC.remove("operation"); MDC.remove("lockKey"); MDC.remove("idempotencyKey")
        return true
    }

    private fun sendCommitWithRetry(peerUrl: String, commitBody: String, attempt: Int = 1) {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$peerUrl/raft/commit"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(commitBody))
            .timeout(Duration.ofSeconds(2))
            .build()

        clusterHttpClient.sendAsyncDiscarding(request)
            .whenComplete { response, error ->
                val success = error == null && response?.statusCode() == 200
                if (success) {
                    peerRepository.markReachable(peerUrl)
                    return@whenComplete
                }
                if (attempt < MAX_COMMIT_RETRIES) {
                    Thread.sleep(COMMIT_RETRY_DELAY_MS * attempt)
                    sendCommitWithRetry(peerUrl, commitBody, attempt + 1)
                } else {
                    MDC.put("peer", peerUrl)
                    MDC.put("error", error?.message ?: "status=${response?.statusCode()}")
                    log.warn("Failed to send commit to peer after retries")
                    peerRepository.markUnreachable(peerUrl)
                    MDC.remove("peer"); MDC.remove("error")
                }
            }
    }

    companion object {
        private const val MAX_COMMIT_RETRIES = 3
        private const val COMMIT_RETRY_DELAY_MS = 200L
    }
}

data class ReplicateRequest(val idempotencyKey: String, val operation: LockOperation, val lock: Lock)
data class CommitRequest(val idempotencyKey: String)
data class JoinRequest(val name: String, val url: String)
data class ExcludeVoteRequest(val suspectUrl: String)
data class ExcludeVoteResponse(val exclude: Boolean)
data class SnapshotResponse(val locks: Collection<Lock>, val queue: Collection<Lock>)
