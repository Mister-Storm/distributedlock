package org.misterstorm.distributedlock.infra.raft.models

import org.misterstorm.distributedlock.core.models.Role
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

@Component
class NodeState(
    @Value("\${distributedlock.node.name}") val nodeName: String,
    @Value("\${distributedlock.node.url}") val nodeUrl: String,
    @Value("\${distributedlock.node.electionTimeout}") private val electionTimeout: Long,
    ) {
    val role: AtomicReference<Role> = AtomicReference(Role.CANDIDATE)
    val currentTerm: AtomicLong = AtomicLong(0)
    val votedFor: AtomicReference<String?> = AtomicReference<String?>(null)
    val leaderId: AtomicReference<String?> = AtomicReference<String?>(null)
    val leaderUrl: AtomicReference<String?> = AtomicReference<String?>(null)
    val lastHeartbeat: AtomicReference<Instant> = AtomicReference(Instant.now())

    fun resetHeartbeatTimer()  = lastHeartbeat.set(Instant.now())

    fun isHeartbeatExpired(): Boolean = Duration.between(
            lastHeartbeat.get(),
            Instant.now()
        ).toMillis() > electionTimeout

    fun isLeader(): Boolean = role.get() == Role.LEADER

    fun incrementTerm() : Long = currentTerm.incrementAndGet()
    fun becomeFollower(
        term: Long,
        leader: String?,
        url: String?,
    ) {
        currentTerm.set(term)
        leaderUrl.set(url)
        leaderId.set(leader)
        role.set(Role.FOLLOWER)
        votedFor.set(null)
        resetHeartbeatTimer()

        println(
            "[$nodeName] became FOLLOWER " +
                    "term=$term leader=$leader"
        )
    }

    fun becomeCandidate() {
        role.set(Role.CANDIDATE)
        incrementTerm()
        votedFor.set(nodeName)
        leaderId.set(null)
        leaderUrl.set(null)
        resetHeartbeatTimer()
        println(
            "[$nodeName] became CANDIDATE " +
                    "term=${currentTerm.get()}"
        )
    }

    fun becomeLeader() {
        role.set(Role.LEADER)
        leaderId.set(nodeName)
        leaderUrl.set(nodeUrl)
        println(
            "[$nodeName] became LEADER " +
                    "term=${currentTerm.get()}"
        )
    }

    fun voteFor(candidateId: String) =
        votedFor.set(candidateId)

    fun clearVotedFor() = votedFor.set(null)

}