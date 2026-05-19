package org.misterstorm.distributedlock.infra.raft

import org.misterstorm.distributedlock.core.models.Role
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@Service
class HeartbeatService constructor(
    private val nodeState: NodeState,
    private val raftProperties: RaftProperties,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
){
    @Scheduled(fixedRate = 1000)
    fun sendHeartbeat() {
        if (nodeState.role.get() != Role.LEADER) {
            return
        }

        val heartbeat = HeartbeatRequest(
            leaderName = nodeState.nodeName,
            term = nodeState.currentTerm.get(),
        )

        val body =
            objectMapper.writeValueAsString(heartbeat)

        raftProperties.peers.forEach { peer ->

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

                println(
                    "Fail heartbeat -> $peer"
                )

                null
            }
        }
    }
}