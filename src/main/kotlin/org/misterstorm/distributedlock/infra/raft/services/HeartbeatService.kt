package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.CommitTracker
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.infra.chaos.ClusterHttpClient
import org.misterstorm.distributedlock.infra.raft.repository.NodeStateRepositoryInMemory
import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@Service
class HeartbeatService(
    private val nodeStateRepository: NodeStateRepositoryInMemory,
    private val clusterHttpClient: ClusterHttpClient,
    private val objectMapper: ObjectMapper,
    private val peerRepository: PeerRepository,
    private val commitTracker: CommitTracker,
) {
    val log: Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRate = 1000)
    fun sendHeartbeat() {
        if (!nodeStateRepository.isLeader()) return

        val state = nodeStateRepository.getState()
        MDC.put("node", state.name)
        MDC.put("term", state.term.toString())
        MDC.put("peers", peerRepository.getRegisteredPeerUrls().size.toString())
        log.info("Sending heartbeat")
        MDC.remove("node"); MDC.remove("term"); MDC.remove("peers")

        val heartbeat = HeartbeatRequest(
            leaderName = state.name,
            term = state.term,
            leaderUrl = state.url,
            recentCommits = commitTracker.getRecentCommits(),
        )
        val body = objectMapper.writeValueAsString(heartbeat)

        peerRepository.getRegisteredPeerUrls().forEach { peer ->
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$peer/raft/heartbeat"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(2))
                .build()

            clusterHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, error ->
                    MDC.put("peer", peer)
                    if (error != null || response?.statusCode() != 200) {
                        peerRepository.markUnhealthy(peer)
                        MDC.put("error", error?.message ?: "status=${response?.statusCode()}")
                        log.warn("Heartbeat to peer failed")
                        MDC.remove("error")
                    } else {
                        peerRepository.markHealthy(peer)
                    }
                    MDC.remove("peer")
                }
        }
    }
}
