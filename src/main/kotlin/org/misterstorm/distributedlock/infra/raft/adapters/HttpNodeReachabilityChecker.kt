package org.misterstorm.distributedlock.infra.raft.adapters

import org.misterstorm.distributedlock.core.adapter.NodeReachabilityChecker
import org.springframework.stereotype.Component
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@Component
class HttpNodeReachabilityChecker(
    private val httpClient: HttpClient,
) : NodeReachabilityChecker {

    override fun isReachable(url: String): Boolean =
        runCatching {
            val response = httpClient.send(
                HttpRequest.newBuilder()
                    .uri(URI.create("$url/raft/status"))
                    .GET()
                    .timeout(Duration.ofSeconds(2))
                    .build(),
                HttpResponse.BodyHandlers.discarding()
            )
            response.statusCode() == 200
        }.getOrDefault(false)
}

