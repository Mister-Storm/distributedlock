package org.misterstorm.distributedlock.core.models.raft

import java.time.Instant

enum class PeerReachability {
    REACHABLE,
    UNREACHABLE,
}

data class PeerEntry(
    val name: String,
    val url: String,
    val reachability: PeerReachability = PeerReachability.REACHABLE,
    val consecutiveFailures: Int = 0,
    val lastSeenAt: Instant? = Instant.now(),
)
