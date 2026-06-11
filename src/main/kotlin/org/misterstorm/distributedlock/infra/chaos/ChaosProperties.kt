package org.misterstorm.distributedlock.infra.chaos

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "distributedlock.chaos")
data class ChaosProperties(
    val enabled: Boolean = false,
    val inbound: ChaosDirectionProperties = ChaosDirectionProperties(),
    val outbound: ChaosDirectionProperties = ChaosDirectionProperties(),
)

data class ChaosDirectionProperties(
    val enabled: Boolean = true,
    val delayMs: Long = 0,
    val mode: ChaosMode = ChaosMode.NONE,
    val offlineMs: Long = 5000,
    val onlineMs: Long = 15000,
)

enum class ChaosMode {
    NONE,
    UNSTABLE_NODE,
    SLOW,
}
