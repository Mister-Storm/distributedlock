package org.misterstorm.distributedlock.infra.chaos

import org.springframework.stereotype.Component
import java.io.IOException
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture

@Component
class ClusterHttpClient(
    private val httpClient: HttpClient,
    private val chaosEngine: ChaosEngine,
) {
    fun <T> send(request: HttpRequest, responseBodyHandler: HttpResponse.BodyHandler<T>): HttpResponse<T> {
        val url = request.uri().toString()
        chaosEngine.applyOutboundDelay()
        if (chaosEngine.isOutboundOffline()) {
            throw IOException("chaos: node temporarily offline (outbound raft traffic to $url)")
        }
        return httpClient.send(request, responseBodyHandler)
    }

    fun sendAsync(
        request: HttpRequest,
        responseBodyHandler: HttpResponse.BodyHandler<String>,
    ): CompletableFuture<HttpResponse<String>> {
        val url = request.uri().toString()
        return try {
            chaosEngine.applyOutboundDelay()
            if (chaosEngine.isOutboundOffline()) {
                CompletableFuture.failedFuture(
                    IOException("chaos: node temporarily offline (outbound raft traffic to $url)")
                )
            } else {
                httpClient.sendAsync(request, responseBodyHandler)
            }
        } catch (ex: InterruptedException) {
            Thread.currentThread().interrupt()
            CompletableFuture.failedFuture(ex)
        }
    }

    fun sendAsyncDiscarding(request: HttpRequest): CompletableFuture<HttpResponse<Void>> {
        val url = request.uri().toString()
        return try {
            chaosEngine.applyOutboundDelay()
            if (chaosEngine.isOutboundOffline()) {
                CompletableFuture.failedFuture(
                    IOException("chaos: node temporarily offline (outbound raft traffic to $url)")
                )
            } else {
                httpClient.sendAsync(request, HttpResponse.BodyHandlers.discarding())
            }
        } catch (ex: InterruptedException) {
            Thread.currentThread().interrupt()
            CompletableFuture.failedFuture(ex)
        }
    }
}
