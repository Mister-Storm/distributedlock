package org.misterstorm.distributedlock.infra.chaos

import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.stereotype.Component
import java.time.Instant

@Component
class ChaosEngine(
    private val chaosProperties: ChaosProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val cycleStartMs = System.currentTimeMillis()
    @Volatile private var lastInboundOffline: Boolean? = null
    @Volatile private var lastOutboundOffline: Boolean? = null

    fun applyOutboundDelay() {
        applyDelay(chaosProperties.outbound)
    }

    fun isOutboundOffline(): Boolean = isOffline(
        chaosProperties.outbound,
        "outbound",
        { lastOutboundOffline },
    ) { lastOutboundOffline = it }

    fun applyInboundDelay() {
        applyDelay(chaosProperties.inbound)
    }

    fun isInboundOffline(): Boolean = isOffline(
        chaosProperties.inbound,
        "inbound",
        { lastInboundOffline },
    ) { lastInboundOffline = it }

    fun snapshot(): Map<String, Any> {
        val inboundOffline = isInboundOffline()
        val outboundOffline = isOutboundOffline()
        return mapOf(
            "enabled" to chaosProperties.enabled,
            "inbound" to directionSnapshot(chaosProperties.inbound, inboundOffline),
            "outbound" to directionSnapshot(chaosProperties.outbound, outboundOffline),
        )
    }

    private fun directionSnapshot(direction: ChaosDirectionProperties, offline: Boolean) = mapOf(
        "enabled" to direction.enabled,
        "delayMs" to direction.delayMs,
        "mode" to direction.mode.name,
        "offlineMs" to direction.offlineMs,
        "onlineMs" to direction.onlineMs,
        "currentlyOffline" to offline,
    )

    private fun applyDelay(direction: ChaosDirectionProperties) {
        if (!chaosProperties.enabled || !direction.enabled) return
        if (direction.mode != ChaosMode.SLOW && direction.delayMs <= 0) return
        val delayMs = when (direction.mode) {
            ChaosMode.SLOW -> direction.delayMs
            ChaosMode.UNSTABLE_NODE -> if (direction.delayMs > 0) direction.delayMs else 0L
            ChaosMode.NONE -> 0L
        }
        if (delayMs > 0) {
            logChaos("DELAY", direction.mode.name, delayMs = delayMs)
            Thread.sleep(delayMs)
        }
    }

    private fun isOffline(
        direction: ChaosDirectionProperties,
        label: String,
        lastState: () -> Boolean?,
        setLastState: (Boolean) -> Unit,
    ): Boolean {
        if (!chaosProperties.enabled || !direction.enabled) return false
        if (direction.mode != ChaosMode.UNSTABLE_NODE) return false

        val cycleMs = direction.offlineMs + direction.onlineMs
        if (cycleMs <= 0) return false

        val elapsed = (System.currentTimeMillis() - cycleStartMs) % cycleMs
        val offline = elapsed < direction.offlineMs
        val previous = lastState()
        if (previous == null || previous != offline) {
            setLastState(offline)
            logChaos(if (offline) "OFFLINE" else "ONLINE", direction.mode.name, label = label)
        }
        return offline
    }

    private fun logChaos(action: String, mode: String, label: String? = null, delayMs: Long? = null) {
        MDC.put("event", "chaos_simulation")
        MDC.put("chaosApplied", "true")
        MDC.put("chaosMode", mode)
        MDC.put("chaosAction", action)
        label?.let { MDC.put("chaosDirection", it) }
        delayMs?.let { MDC.put("chaosDelayMs", it.toString()) }
        log.warn("Chaos simulation: node is now $action")
        MDC.remove("event")
        MDC.remove("chaosApplied")
        MDC.remove("chaosMode")
        MDC.remove("chaosAction")
        MDC.remove("chaosDirection")
        MDC.remove("chaosDelayMs")
    }
}
