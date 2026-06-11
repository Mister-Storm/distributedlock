package org.misterstorm.distributedlock.core.models.raft

import java.time.Instant

enum class PeerReachability {
    HEALTHY,
    UNHEALTHY,
}

data class PeerEntry(
    val name: String,
    val url: String,
    val reachability: PeerReachability = PeerReachability.HEALTHY,
    val consecutiveFailures: Int = 0,
    val lastSeenAt: Instant? = Instant.now(),
    val lastHealthCheckAt: Instant? = null,
)
