package org.misterstorm.distributedlock.infra.raft

data class HeartbeatRequest(
    val leaderName: String,
    val term: Long,
)
