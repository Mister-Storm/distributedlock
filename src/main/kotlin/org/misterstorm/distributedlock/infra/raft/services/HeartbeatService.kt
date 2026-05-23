package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.slf4j.Logger
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
import java.util.concurrent.ConcurrentLinkedQueue

@Service
class HeartbeatService (
    private val nodeState: NodeState,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
    private val nodeRegistry: NodeRegistry,
){
    val log : Logger = LoggerFactory.getLogger(javaClass)
    companion object {
        private const val MAX_FAILURES = 3
        private const val MAX_RECENT_COMMITS = 50
    }
    private val recentCommits = ConcurrentLinkedQueue<String>()
    private val failureCount = ConcurrentHashMap<String, Int>()

    fun recordCommit(idempotencyKey: String) {
        recentCommits.add(idempotencyKey)
        while(recentCommits.size > MAX_RECENT_COMMITS) {
            recentCommits.poll()
        }
    }

    @Scheduled(fixedRate = 1000)
    fun sendHeartbeat() {
        if (!nodeState.isLeader()) {
            return
        }
        log.info("Sending heartbeat")
        val heartbeat = HeartbeatRequest(
            leaderName = nodeState.nodeName,
            term = nodeState.currentTerm.get(),
            leaderUrl = nodeState.nodeUrl,
            recentCommits = recentCommits.toList(),
        )

        val body =
            objectMapper.writeValueAsString(heartbeat)

        nodeRegistry.getPeerUrls().forEach { peer ->

            val request = HttpRequest.newBuilder()
                .uri(
                    URI.create(
                        "$peer/raft/heartbeat"
                    )
                )
                .header(
                    "Content-Type",
                    "application/json"
                )
                .POST(
                    HttpRequest.BodyPublishers
                        .ofString(body)
                )
                .timeout(Duration.ofSeconds(2))
                .build()

            httpClient.sendAsync(
                request,
                HttpResponse.BodyHandlers.discarding()
            ).exceptionally {
                val failures = failureCount.merge(peer, 1, Int::plus) ?: 1
                if (failures >= MAX_FAILURES) {
                    nodeRegistry.remove(peer)
                    failureCount.remove(peer)
                    println("Node $peer removed after $MAX_FAILURES failed heartbeats")
                }
                null
            }
            failureCount.remove(peer)
        }
    }
}