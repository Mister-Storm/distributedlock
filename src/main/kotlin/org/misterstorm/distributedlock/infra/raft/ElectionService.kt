package org.misterstorm.distributedlock.infra.raft

import org.misterstorm.distributedlock.core.models.Role
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.math.log

@Service
class ElectionService(private val nodeState: NodeState,
                      private val raftProperties: RaftProperties,
                      private val httpClient: HttpClient,
                      private val objectMapper: ObjectMapper
) {
    @Scheduled(fixedRate = 60000)
    fun checkElectionTimeout() {
        if(nodeState.role.get() == Role.LEADER || !nodeState.isHeartbeatExpired()) {
            return
        }
        startElection()
    }
    fun startElection() {
        synchronized(nodeState) {
            nodeState.becomeCandidate()
            val voteRequest = VoteRequest(
                candidateName = nodeState.nodeName,
                term = nodeState.currentTerm.get(),
            )
            val body = objectMapper.writeValueAsString(voteRequest)
            var votes = 1
            raftProperties.peers.forEach { peer ->
                runCatching {
                    val request = HttpRequest.newBuilder()
                        .uri(java.net.URI.create("$peer/raft/vote"))
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
            val quorum = (raftProperties.peers.size + 1) / 2 + 1
            if (votes >= quorum) {
                nodeState.becomeLeader()
            } else {
                println("Election failed, received $votes votes, needed $quorum")
            }
        }
    }
}