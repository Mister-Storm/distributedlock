package org.misterstorm.distributedlock.infra.chaos

import jakarta.servlet.Filter
import jakarta.servlet.FilterChain
import jakarta.servlet.ServletRequest
import jakarta.servlet.ServletResponse
import jakarta.servlet.http.HttpServletResponse
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(1)
@ConditionalOnProperty(prefix = "distributedlock.chaos", name = ["enabled"], havingValue = "true")
class ChaosFilter(
    private val chaosEngine: ChaosEngine,
) : Filter {

    override fun doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain) {
        val httpResponse = response as HttpServletResponse
        val path = (request as jakarta.servlet.http.HttpServletRequest).requestURI

        if (!path.startsWith("/raft")) {
            chain.doFilter(request, response)
            return
        }

        chaosEngine.applyInboundDelay()
        if (chaosEngine.isInboundOffline()) {
            httpResponse.status = HttpServletResponse.SC_SERVICE_UNAVAILABLE
            httpResponse.contentType = "application/json"
            httpResponse.writer.write("""{"error":"chaos: node temporarily offline (raft only)"}""")
            return
        }

        chain.doFilter(request, response)
    }
}
