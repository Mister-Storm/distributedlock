package org.misterstorm.distributedlock.infra.chaos

import jakarta.servlet.Filter
import jakarta.servlet.FilterChain
import jakarta.servlet.ServletRequest
import jakarta.servlet.ServletResponse
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(1)
@ConditionalOnProperty(prefix = "distributedlock.chaos", name = ["enabled"], havingValue = "true")
class ChaosFilter(
    private val chaosEngine: ChaosEngine,
    @Value("\${server.port:8080}") private val serverPort: Int,
) : Filter {

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val httpRequest = request as HttpServletRequest
        val httpResponse = response as HttpServletResponse
        val path = httpRequest.requestURI

        if (path.startsWith("/raft") || path.startsWith("/lock")) {
            chaosEngine.applyInboundDelay(serverPort)
            if (chaosEngine.shouldFailInbound(serverPort)) {
                httpResponse.status = HttpServletResponse.SC_SERVICE_UNAVAILABLE
                httpResponse.contentType = "application/json"
                httpResponse.writer.write("""{"error":"chaos: simulated inbound failure"}""")
                return
            }
        }

        chain.doFilter(request, response)
    }
}
