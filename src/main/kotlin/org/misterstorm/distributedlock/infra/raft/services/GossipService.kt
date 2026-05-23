package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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
    private val objectMapper: ObjectMapper
) {
    val log : Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "#{'\${raft.gossipInterval:5000}'}")
    fun executeGossip() {
        log.info("Starting gossip")
        val localNodes = nodeRegistry.getAllNodes()
        val deadNodes = nodeRegistry.getPendingRemovals()
        val requestBody = objectMapper.writeValueAsString(GossipMessage(localNodes, deadNodes))
        nodeRegistry.clearPendingRemovals()
        nodeRegistry.getPeerUrls().forEach { url ->
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$url/raft/gossip"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build()
                httpClient.send(request, HttpResponse.BodyHandlers.ofString()).let {
                    if (it.statusCode() == 200) {
                        log.info("Gossip received")
                        val response = objectMapper.readValue(it.body(), GossipMessage::class.java)
                        nodeRegistry.merge(response.nodes)
                        nodeRegistry.applyRemovals(response.deadNodes)
                    } else {
                        nodeRegistry.remove(url)
                    }
                }
            }.onFailure {
                log.warn("Failed to gossip with $url: ${it.message}")
            }
        }
    }
}

data class GossipMessage(
    val nodes: Map<String, String>,
    val deadNodes: Set<String> = emptySet(),
)