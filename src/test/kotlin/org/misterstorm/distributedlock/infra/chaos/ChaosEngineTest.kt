package org.misterstorm.distributedlock.infra.chaos

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ChaosEngineTest {

    @Test
    fun `should alternate failures for odd port outbound`() {
        val engine = ChaosEngine(
            ChaosProperties(
                enabled = true,
                outbound = ChaosDirectionProperties(
                    enabled = true,
                    mode = ChaosMode.ALTERNATING_ODD_PORTS,
                ),
            )
        )
        val oddUrl = "http://localhost:8081/raft/status"
        val evenUrl = "http://localhost:8082/raft/status"

        assertFalse(engine.shouldFailOutbound(evenUrl))
        assertFalse(engine.shouldFailOutbound(evenUrl))

        val results = (1..4).map { engine.shouldFailOutbound(oddUrl) }
        assertTrue(results[0])
        assertFalse(results[1])
        assertTrue(results[2])
        assertFalse(results[3])
    }

    @Test
    fun `should not fail when chaos disabled`() {
        val engine = ChaosEngine(ChaosProperties(enabled = false))
        assertFalse(engine.shouldFailOutbound("http://localhost:8081/raft/status"))
    }
}
