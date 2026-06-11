package org.misterstorm.distributedlock.infra.chaos

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ChaosEngineTest {

    @Test
    fun `should cycle offline and online for unstable node outbound`() {
        val engine = ChaosEngine(
            ChaosProperties(
                enabled = true,
                outbound = ChaosDirectionProperties(
                    enabled = true,
                    mode = ChaosMode.UNSTABLE_NODE,
                    offlineMs = 100,
                    onlineMs = 200,
                ),
            ),
        )

        assertTrue(engine.isOutboundOffline())
        Thread.sleep(110)
        assertFalse(engine.isOutboundOffline())
        Thread.sleep(200)
        assertTrue(engine.isOutboundOffline())
    }

    @Test
    fun `should cycle offline and online for unstable node inbound`() {
        val engine = ChaosEngine(
            ChaosProperties(
                enabled = true,
                inbound = ChaosDirectionProperties(
                    enabled = true,
                    mode = ChaosMode.UNSTABLE_NODE,
                    offlineMs = 50,
                    onlineMs = 150,
                ),
            ),
        )

        assertTrue(engine.isInboundOffline())
        Thread.sleep(60)
        assertFalse(engine.isInboundOffline())
    }

    @Test
    fun `should not fail when chaos disabled`() {
        val engine = ChaosEngine(ChaosProperties(enabled = false))
        assertFalse(engine.isOutboundOffline())
        assertFalse(engine.isInboundOffline())
    }

    @Test
    fun `should not fail when mode is none`() {
        val engine = ChaosEngine(
            ChaosProperties(
                enabled = true,
                outbound = ChaosDirectionProperties(enabled = true, mode = ChaosMode.NONE),
            ),
        )
        assertFalse(engine.isOutboundOffline())
    }
}
