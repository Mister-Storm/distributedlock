package org.misterstorm.distributedlock.infra.raft.requests

data class VoteRequest(
    val candidateName: String,
    val candidateUrl: String,
    val term: Long,
)