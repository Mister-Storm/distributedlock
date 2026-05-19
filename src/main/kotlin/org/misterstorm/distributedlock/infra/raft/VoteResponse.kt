package org.misterstorm.distributedlock.infra.raft

data class VoteResponse(
    val term: Long,
    val voteGranted: Boolean,
)
