package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.requests.VoteRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteResponse
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Service
class ElectionService(private val nodeState: NodeState,
                      private val nodeRegistry: NodeRegistry,
                      private val httpClient: HttpClient,
                      private val objectMapper: ObjectMapper
) {
    @Scheduled(fixedRate = 60000)
    fun checkElectionTimeout() {
        if(nodeState.isLeader() || !nodeState.isHeartbeatExpired()) {
            return
        }
        startElection()
    }
    fun startElection() {
        synchronized(nodeState) {
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
                    if (voteResponse.voteGranted) {
                        votes++
                    }
                }.onFailure {

                }
            }
            val quorum = (nodeRegistry.getPeerUrls().size + 1) / 2 + 1
            if (votes >= quorum) {
                nodeState.becomeLeader()
            } else {
                println("Election failed, received $votes votes, needed $quorum")
            }
        }
    }
}