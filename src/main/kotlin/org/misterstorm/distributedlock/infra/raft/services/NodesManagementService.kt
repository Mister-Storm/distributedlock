package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.slf4j.LoggerFactory
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
class NodesManagementService(
    private val nodeRegistry: NodeRegistry,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val failuresNodes = ConcurrentHashMap<String, Int>()

    @Scheduled(fixedRate = 4000)
    fun manage() {
        nodeRegistry.getPeerUrls().forEach { peer ->
            runCatching {
                val response = httpClient.send(
                    HttpRequest.newBuilder()
                        .uri(URI.create("$peer/raft/status"))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString()
                )
                if (response.statusCode() != 200) {
                    logger.warn("Node $peer is unresponsive")
                    failuresNodes.incrementFailure(peer)
                } else {
                    failuresNodes.resetFail(peer)
                }
            }.onFailure {
                logger.warn("Failed to ping node $peer: ${it.message}")
                failuresNodes.incrementFailure(peer)
            }

        }
        startExclusionProcess()
    }

    private fun startExclusionProcess() {
        val excludeCandidates = failuresNodes.filterValues { it > MAX_FAILURES }.keys

        excludeCandidates.forEach { suspectUrl ->
            val healthyPeers = nodeRegistry.getPeerUrls().filter { it != suspectUrl }

            var excludeVotes = 1
            var totalVoters = 1

            val requestBody = objectMapper.writeValueAsString(ExcludeVoteRequest(suspectUrl))

            healthyPeers.forEach { peer ->
                runCatching {
                    val response = httpClient.send(
                        HttpRequest.newBuilder()
                            .uri(URI.create("$peer/raft/exclude-vote"))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                            .timeout(Duration.ofSeconds(2))
                            .build(),
                        HttpResponse.BodyHandlers.ofString()
                    )
                    if (response.statusCode() == 200) {
                        totalVoters++
                        val vote = objectMapper.readValue(response.body(), ExcludeVoteResponse::class.java)
                        if (vote.exclude) excludeVotes++
                    }
                }.onFailure {
                    logger.warn("Failed to get exclude vote from $peer: ${it.message}")
                }
            }

            val quorum = totalVoters / 2 + 1
            if (excludeVotes >= quorum) {
                logger.warn("Quorum reached ($excludeVotes/$totalVoters): excluding $suspectUrl")
                nodeRegistry.remove(suspectUrl)
                failuresNodes.remove(suspectUrl)
            } else {
                logger.info("Quorum NOT reached ($excludeVotes/$totalVoters): keeping $suspectUrl, resetting failures")
                failuresNodes.resetFail(suspectUrl)
            }
        }
    }

    fun ConcurrentHashMap<String, Int>.incrementFailure(node: String) =
        this.merge(node, 1) { old, _ -> old + 1 }
    fun ConcurrentHashMap<String, Int>.resetFail(node: String) =
        this.merge(node, 0) { _, _ -> 0 }

    companion object {
        const val MAX_FAILURES = 5
    }
}