package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
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

@Service
class GossipService(
    private val nodeRegistry: NodeRegistry,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
) {
    val log: Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "#{'\${raft.gossipInterval:5000}'}")
    fun executeGossip() {
        val localNodes = nodeRegistry.getAllNodes()
        val deadNodes = nodeRegistry.getPendingRemovals()

        MDC.put("knownNodes", localNodes.size.toString())
        MDC.put("deadNodes", deadNodes.size.toString())
        log.info("Starting gossip round")
        MDC.remove("knownNodes"); MDC.remove("deadNodes")

        val requestBody = objectMapper.writeValueAsString(GossipMessage(localNodes, deadNodes))
        nodeRegistry.clearPendingRemovals()

        nodeRegistry.getPeerUrls().forEach { url ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$url/raft/gossip"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build()
                httpClient.send(request, HttpResponse.BodyHandlers.ofString()).let { response ->
                    MDC.put("peer", url)
                    if (response.statusCode() == 200) {
                        val gossipResponse = objectMapper.readValue(response.body(), GossipMessage::class.java)
                        nodeRegistry.merge(gossipResponse.nodes)
                        nodeRegistry.applyRemovals(gossipResponse.deadNodes)
                        MDC.put("receivedNodes", gossipResponse.nodes.size.toString())
                        MDC.put("receivedDeadNodes", gossipResponse.deadNodes.size.toString())
                        log.info("Gossip exchange successful")
                        MDC.remove("receivedNodes"); MDC.remove("receivedDeadNodes")
                    } else {
                        MDC.put("statusCode", response.statusCode().toString())
                        log.warn("Gossip rejected by peer, removing it")
                        MDC.remove("statusCode")
                        nodeRegistry.remove(url)
                    }
                    MDC.remove("peer")
                }
            }.onFailure { ex ->
                MDC.put("peer", url)
                MDC.put("error", ex.message)
                log.warn("Gossip failed for peer")
                MDC.remove("peer"); MDC.remove("error")
            }
        }
    }
}

data class GossipMessage(
    val nodes: Map<String, String>,
    val deadNodes: Set<String> = emptySet(),
)