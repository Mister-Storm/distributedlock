package org.misterstorm.distributedlock.infra.raft.adapters

import org.misterstorm.distributedlock.core.adapter.VoteRequester
import org.misterstorm.distributedlock.core.models.raft.VoteInput
import org.misterstorm.distributedlock.core.models.raft.VoteOutput
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

@Component
class HttpVoteRequester(
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
) : VoteRequester {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun requestVote(peerUrl: String, input: VoteInput): VoteOutput? =
        runCatching {
            val body = objectMapper.writeValueAsString(input)
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$peerUrl/raft/vote"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build()
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            objectMapper.readValue(response.body(), VoteOutput::class.java)
        }.getOrElse { ex ->
            MDC.put("peer", peerUrl); MDC.put("error", ex.message)
            log.warn("Failed to request vote from peer")
            MDC.remove("peer"); MDC.remove("error")
            null
        }
}

