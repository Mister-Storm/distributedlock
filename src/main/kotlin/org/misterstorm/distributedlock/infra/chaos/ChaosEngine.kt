package org.misterstorm.distributedlock.infra.chaos

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.net.URI
import java.util.concurrent.atomic.AtomicLong

@Component
class ChaosEngine(
    private val chaosProperties: ChaosProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val outboundCounter = AtomicLong(0)
    private val inboundCounter = AtomicLong(0)

    fun applyOutboundDelay(url: String) {
        applyDelay(chaosProperties.outbound, url, "outbound")
    }

    fun shouldFailOutbound(url: String): Boolean =
        shouldFail(chaosProperties.outbound, url, outboundCounter, "outbound")

    fun applyInboundDelay(localPort: Int) {
        if (!chaosProperties.enabled || !chaosProperties.inbound.enabled) return
        val delayMs = chaosProperties.inbound.delayMs
        if (delayMs <= 0) return
        if (shouldApplyToPort(chaosProperties.inbound.mode, localPort, urlPort = localPort)) {
            applyDelayWithLogging(delayMs, "inbound", localPort)
        }
    }

    fun shouldFailInbound(localPort: Int): Boolean {
        if (!chaosProperties.enabled || !chaosProperties.inbound.enabled) return false
        return shouldFail(chaosProperties.inbound, portAsUrl(localPort), inboundCounter, "inbound")
    }

    fun snapshot(): Map<String, Any> = mapOf(
        "enabled" to chaosProperties.enabled,
        "inbound" to mapOf(
            "enabled" to chaosProperties.inbound.enabled,
            "delayMs" to chaosProperties.inbound.delayMs,
            "mode" to chaosProperties.inbound.mode.name,
        ),
        "outbound" to mapOf(
            "enabled" to chaosProperties.outbound.enabled,
            "delayMs" to chaosProperties.outbound.delayMs,
            "mode" to chaosProperties.outbound.mode.name,
        ),
    )

    private fun applyDelay(direction: ChaosDirectionProperties, url: String, label: String) {
        if (!chaosProperties.enabled || !direction.enabled) return
        val port = URI(url).port
        if (direction.delayMs <= 0) return
        if (shouldApplyToPort(direction.mode, port, port)) {
            applyDelayWithLogging(direction.delayMs, label, port)
        }
    }

    private fun shouldFail(
        direction: ChaosDirectionProperties,
        url: String,
        counter: AtomicLong,
        label: String,
    ): Boolean {
        if (!chaosProperties.enabled || !direction.enabled) return false
        val port = URI(url).port
        if (port <= 0) return false

        val fail = when (direction.mode) {
            ChaosMode.NONE -> false
            ChaosMode.ALWAYS_FAIL_ODD_PORTS -> port % 2 != 0
            ChaosMode.ALTERNATING_ODD_PORTS -> {
                if (port % 2 == 0) false
                else counter.getAndIncrement() % 2 == 0L
            }
        }

        if (fail) {
            MDC.put("chaosApplied", "true")
            MDC.put("chaosMode", direction.mode.name)
            MDC.put("chaosAction", "FAIL")
            MDC.put("chaosDirection", label)
            MDC.put("chaosPort", port.toString())
            log.warn("Chaos simulated failure")
            MDC.remove("chaosApplied")
            MDC.remove("chaosMode")
            MDC.remove("chaosAction")
            MDC.remove("chaosDirection")
            MDC.remove("chaosPort")
        }
        return fail
    }

    private fun applyDelayWithLogging(delayMs: Long, label: String, port: Int) {
        MDC.put("chaosApplied", "true")
        MDC.put("chaosAction", "DELAY")
        MDC.put("chaosDirection", label)
        MDC.put("chaosPort", port.toString())
        MDC.put("chaosDelayMs", delayMs.toString())
        log.warn("Chaos applying delay")
        MDC.remove("chaosApplied")
        MDC.remove("chaosAction")
        MDC.remove("chaosDirection")
        MDC.remove("chaosPort")
        MDC.remove("chaosDelayMs")
        Thread.sleep(delayMs)
    }

    private fun shouldApplyToPort(mode: ChaosMode, port: Int, urlPort: Int): Boolean =
        when (mode) {
            ChaosMode.NONE -> true
            ChaosMode.ALWAYS_FAIL_ODD_PORTS, ChaosMode.ALTERNATING_ODD_PORTS -> urlPort % 2 != 0 || port % 2 != 0
        }

    private fun portAsUrl(port: Int): String = "http://localhost:$port"
}
