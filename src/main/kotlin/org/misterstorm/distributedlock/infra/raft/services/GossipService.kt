package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.infra.chaos.ClusterHttpClient
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Service
class GossipService(
    private val peerRepository: PeerRepository,
    private val clusterHttpClient: ClusterHttpClient,
    private val objectMapper: ObjectMapper,
) {
    val log: Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "#{'\${distributedlock.node.gossipInterval:3000}'}")
    fun executeGossip() {
        val localNodes = peerRepository.getAllNodes()

        MDC.put("knownNodes", localNodes.size.toString())
        log.info("Starting gossip round")
        MDC.remove("knownNodes")

        val requestBody = objectMapper.writeValueAsString(GossipMessage(localNodes))

        peerRepository.getRegisteredPeerUrls().forEach { url ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$url/raft/gossip"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build()
                clusterHttpClient.send(request, HttpResponse.BodyHandlers.ofString()).let { response ->
                    MDC.put("peer", url)
                    if (response.statusCode() == 200) {
                        val gossipResponse = objectMapper.readValue(response.body(), GossipMessage::class.java)
                        peerRepository.merge(gossipResponse.nodes)
                        peerRepository.markHealthy(url)
                        MDC.put("receivedNodes", gossipResponse.nodes.size.toString())
                        log.info("Gossip exchange successful")
                        MDC.remove("receivedNodes")
                    } else {
                        MDC.put("statusCode", response.statusCode().toString())
                        log.warn("Gossip rejected by peer")
                        peerRepository.markUnhealthy(url)
                        MDC.remove("statusCode")
                    }
                    MDC.remove("peer")
                }
            }.onFailure { ex ->
                MDC.put("peer", url)
                MDC.put("error", ex.message)
                log.warn("Gossip failed for peer")
                peerRepository.markUnhealthy(url)
                MDC.remove("peer"); MDC.remove("error")
            }
        }
    }
}

data class GossipMessage(
    val nodes: Map<String, String>,
)
