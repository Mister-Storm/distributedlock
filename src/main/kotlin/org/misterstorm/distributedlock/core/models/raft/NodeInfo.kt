package org.misterstorm.distributedlock.core.models.raft

import org.misterstorm.distributedlock.core.models.Role
import java.time.Instant

data class NodeInfo(
    val name: String,
    val url: String,
    val role: Role = Role.CANDIDATE,
    val term: Long = 0L,
    val votedFor: String? = null,
    val leaderName: String? = null,
    val leaderUrl: String? = null,
    val lastHeartbeatAt: Instant = Instant.now(),
) {
    fun asLeader(): NodeInfo = copy(
        role = Role.LEADER,
        leaderName = name,
        leaderUrl = url,
    )

    fun asFollower(term: Long, leaderName: String?, leaderUrl: String?): NodeInfo = copy(
        role = Role.FOLLOWER,
        term = term,
        leaderName = leaderName,
        leaderUrl = leaderUrl,
        votedFor = null,
        lastHeartbeatAt = Instant.now(),
    )

    fun asFollowerAfterFailedElection(): NodeInfo = copy(
        role = Role.FOLLOWER,
        leaderName = null,
        leaderUrl = null,
        lastHeartbeatAt = Instant.now(),
    )

    fun asCandidate(): NodeInfo = copy(
        role = Role.CANDIDATE,
        term = term + 1,
        votedFor = name,
        leaderName = null,
        leaderUrl = null,
        lastHeartbeatAt = Instant.now(),
    )

    fun withVote(candidateId: String): NodeInfo = copy(votedFor = candidateId)
    fun withoutVote(): NodeInfo = copy(votedFor = null)
    fun isLeader(): Boolean = role == Role.LEADER
}
