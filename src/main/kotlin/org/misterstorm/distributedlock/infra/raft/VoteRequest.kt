package org.misterstorm.distributedlock.infra.raft

data class VoteRequest(
    val candidateName: String,
    val candidateUrl: String,
    val term: Long,
)
