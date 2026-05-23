package org.misterstorm.distributedlock.infra.raft.requests

data class VoteResponse(
    val term: Long,
    val voteGranted: Boolean,
)