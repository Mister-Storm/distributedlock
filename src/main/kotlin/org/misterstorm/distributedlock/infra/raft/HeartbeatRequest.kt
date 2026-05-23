package org.misterstorm.distributedlock.infra.raft

data class HeartbeatRequest(
    val leaderName: String,
    val leaderUrl: String,
    val term: Long,
    val recentCommits: List<String> = emptyList()
)
