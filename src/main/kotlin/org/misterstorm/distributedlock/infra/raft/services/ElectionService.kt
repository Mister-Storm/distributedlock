package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.requests.VoteRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteResponse
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
class ElectionService(
    private val nodeState: NodeState,
    private val nodeRegistry: NodeRegistry,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "\${distributedlock.node.electionTimeout:6000}")
    fun checkElectionTimeout() {
        if (nodeState.isLeader() || !nodeState.isHeartbeatExpired()) return
        startElection()
    }

    fun startElection() {
        synchronized(nodeState) {
            MDC.put("node", nodeState.nodeName)
            MDC.put("term", (nodeState.currentTerm.get() + 1).toString())
            log.info("Starting election")

            nodeState.becomeCandidate()
            val voteRequest = VoteRequest(
                candidateName = nodeState.nodeName,
                candidateUrl = nodeState.nodeUrl,
                term = nodeState.currentTerm.get(),
            )
            val body = objectMapper.writeValueAsString(voteRequest)
            var votes = 1

            nodeRegistry.getPeerUrls().forEach { peer ->
                runCatching {
                    val request = HttpRequest.newBuilder()
                        .uri(URI.create("$peer/raft/vote"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build()
                    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                    val voteResponse = objectMapper.readValue(response.body(), VoteResponse::class.java)
                    MDC.put("peer", peer)
                    if (voteResponse.voteGranted) {
                        votes++
                        log.info("Vote granted by peer")
                    } else {
                        log.info("Vote denied by peer")
                    }
                    MDC.remove("peer")
                }.onFailure { ex ->
                    MDC.put("peer", peer)
                    MDC.put("error", ex.message)
                    log.warn("Failed to request vote from peer")
                    MDC.remove("peer"); MDC.remove("error")
                }
            }

            val quorum = (nodeRegistry.getPeerUrls().size + 1) / 2 + 1
            MDC.put("votes", votes.toString())
            MDC.put("quorum", quorum.toString())
            if (votes >= quorum) {
                log.info("Election won")
                nodeState.becomeLeader()
            } else {
                log.warn("Election failed: quorum not reached")
            }
            MDC.remove("node"); MDC.remove("term"); MDC.remove("votes"); MDC.remove("quorum")
        }
    }
}