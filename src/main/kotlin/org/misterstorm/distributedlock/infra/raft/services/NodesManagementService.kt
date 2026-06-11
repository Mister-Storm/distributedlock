package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.support.ClusterHealth
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

    @Scheduled(fixedRateString = "#{'\${distributedlock.health.checkInterval:4000}'}")
    fun checkClusterHealth() {
        val registered = peerRepository.getRegisteredPeerUrls()

        registered.forEach { peer -> probePeer(peer) }

        val healthy = peerRepository.getHealthyPeerUrls()
        val unhealthy = peerRepository.getUnhealthyPeerUrls()

        MDC.put("event", "cluster_health_summary")
        MDC.put("registeredPeers", registered.size.toString())
        MDC.put("healthyPeers", healthy.size.toString())
        MDC.put("unhealthyPeers", unhealthy.size.toString())
        MDC.put("healthyClusterSize", ClusterHealth.healthyClusterSize(peerRepository).toString())
        logger.info("Cluster health check completed")
        MDC.remove("event")
        MDC.remove("registeredPeers")
        MDC.remove("healthyPeers")
        MDC.remove("unhealthyPeers")
        MDC.remove("healthyClusterSize")

        unhealthy.forEach { peer ->
            MDC.put("event", "peer_recovery_probe")
            MDC.put("peer", peer)
            logger.info("Unhealthy peer scheduled for recovery probe")
            MDC.remove("event")
            MDC.remove("peer")
        }
    }

    private fun probePeer(peer: String) {
        runCatching {
            val response = clusterHttpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("$peer/raft/status"))
                    .GET()
                    .timeout(java.time.Duration.ofSeconds(2))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            )
            MDC.put("peer", peer)
            MDC.put("event", "peer_health_probe")
            if (response.statusCode() != 200) {
                MDC.put("statusCode", response.statusCode().toString())
                logger.warn("Peer health check returned non-200")
                peerRepository.markUnhealthy(peer)
                MDC.remove("statusCode")
            } else {
                peerRepository.markHealthy(peer)
            }
            MDC.remove("peer")
            MDC.remove("event")
        }.onFailure { ex ->
            MDC.put("peer", peer)
            MDC.put("event", "peer_health_probe")
            MDC.put("error", ex.message)
            logger.warn("Failed to probe peer health")
            peerRepository.markUnhealthy(peer)
            MDC.remove("peer")
            MDC.remove("event")
            MDC.remove("error")
        }
    }
}
