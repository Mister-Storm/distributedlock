package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.CommitTracker
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.infra.raft.repository.NodeStateRepositoryInMemory
import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Service
class HeartbeatService(
    private val nodeStateRepository: NodeStateRepositoryInMemory,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
    private val peerRepository: PeerRepository,
    private val commitTracker: CommitTracker,
) {
    val log: Logger = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val MAX_FAILURES = 3
    }

    private val failureCount = ConcurrentHashMap<String, Int>()


    @Scheduled(fixedRate = 1000)
    fun sendHeartbeat() {
        if (!nodeStateRepository.isLeader()) return

        val state = nodeStateRepository.getState()
        MDC.put("node", state.name)
        MDC.put("term", state.term.toString())
        MDC.put("peers", peerRepository.getPeerUrls().size.toString())
        log.info("Sending heartbeat")
        MDC.remove("node"); MDC.remove("term"); MDC.remove("peers")

        val heartbeat = HeartbeatRequest(
            leaderName = state.name,
            term = state.term,
            leaderUrl = state.url,
            recentCommits = commitTracker.getRecentCommits(),
        )
        val body = objectMapper.writeValueAsString(heartbeat)

        peerRepository.getPeerUrls().forEach { peer ->
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$peer/raft/heartbeat"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(2))
                .build()

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding()).exceptionally {
                val failures = failureCount.merge(peer, 1, Int::plus) ?: 1
                MDC.put("peer", peer)
                MDC.put("failures", failures.toString())
                MDC.put("maxFailures", MAX_FAILURES.toString())
                if (failures >= MAX_FAILURES) {
                    peerRepository.remove(peer)
                    failureCount.remove(peer)
                    log.warn("Peer removed after consecutive heartbeat failures")
                } else {
                    log.warn("Heartbeat to peer failed")
                }
                MDC.remove("peer"); MDC.remove("failures"); MDC.remove("maxFailures")
                null
            }
            failureCount.remove(peer)
        }
    }
}