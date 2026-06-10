package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.infra.chaos.ClusterHttpClient
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Service
class NodesManagementService(
    private val peerRepository: PeerRepository,
    private val clusterHttpClient: ClusterHttpClient,
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRate = 4000)
    fun manage() {
        peerRepository.getPeerUrls().forEach { peer ->
            runCatching {
                val response = clusterHttpClient.send(
                    HttpRequest.newBuilder()
                        .uri(URI.create("$peer/raft/status"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                )
                MDC.put("peer", peer)
                if (response.statusCode() != 200) {
                    MDC.put("statusCode", response.statusCode().toString())
                    logger.warn("Peer health check returned non-200")
                    peerRepository.markUnreachable(peer)
                    MDC.remove("statusCode")
                } else {
                    peerRepository.markReachable(peer)
                }
                MDC.remove("peer")
            }.onFailure { ex ->
                MDC.put("peer", peer)
                MDC.put("error", ex.message)
                logger.warn("Failed to ping peer")
                peerRepository.markUnreachable(peer)
                MDC.remove("peer"); MDC.remove("error")
            }
        }
    }
}
