package org.misterstorm.distributedlock.infra.raft.models

import org.misterstorm.distributedlock.core.adapter.LeaderStatus
import org.misterstorm.distributedlock.core.models.Role
import org.slf4j.LoggerFactory
import org.slf4j.MDC
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
): LeaderStatus {
    private val log = LoggerFactory.getLogger(javaClass)
    val role: AtomicReference<Role> = AtomicReference(Role.CANDIDATE)
    val currentTerm: AtomicLong = AtomicLong(0)
    val votedFor: AtomicReference<String?> = AtomicReference<String?>(null)
    val leaderId: AtomicReference<String?> = AtomicReference<String?>(null)
    val leaderUrl: AtomicReference<String?> = AtomicReference<String?>(null)
    val lastHeartbeat: AtomicReference<Instant> = AtomicReference(Instant.now())

    fun resetHeartbeatTimer() = lastHeartbeat.set(Instant.now())

    fun isHeartbeatExpired(): Boolean = Duration.between(
        lastHeartbeat.get(), Instant.now()
    ).toMillis() > electionTimeout

    override fun isLeader(): Boolean = role.get() == Role.LEADER
    override fun getLeaderUrl(): String? = leaderUrl.get()
    fun incrementTerm(): Long = currentTerm.incrementAndGet()

    fun becomeFollower(term: Long, leader: String?, url: String?) {
        currentTerm.set(term)
        leaderUrl.set(url)
        leaderId.set(leader)
        role.set(Role.FOLLOWER)
        votedFor.set(null)
        resetHeartbeatTimer()

        MDC.put("node", nodeName)
        MDC.put("term", term.toString())
        MDC.put("leader", leader)
        MDC.put("leaderUrl", url)
        log.info("Node transitioned to FOLLOWER")
        MDC.remove("node"); MDC.remove("term"); MDC.remove("leader"); MDC.remove("leaderUrl")
    }

    fun becomeCandidate() {
        role.set(Role.CANDIDATE)
        incrementTerm()
        votedFor.set(nodeName)
        leaderId.set(null)
        leaderUrl.set(null)
        resetHeartbeatTimer()

        MDC.put("node", nodeName)
        MDC.put("term", currentTerm.get().toString())
        log.info("Node transitioned to CANDIDATE")
        MDC.remove("node"); MDC.remove("term")
    }

    fun becomeLeader() {
        role.set(Role.LEADER)
        leaderId.set(nodeName)
        leaderUrl.set(nodeUrl)

        MDC.put("node", nodeName)
        MDC.put("term", currentTerm.get().toString())
        log.info("Node transitioned to LEADER")
        MDC.remove("node"); MDC.remove("term")
    }

    fun voteFor(candidateId: String) = votedFor.set(candidateId)

    fun clearVotedFor() = votedFor.set(null)

}