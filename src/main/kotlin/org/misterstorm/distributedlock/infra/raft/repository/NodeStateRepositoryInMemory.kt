package org.misterstorm.distributedlock.infra.raft.repository

import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

@Component
class NodeStateRepositoryInMemory(
    @Value("\${distributedlock.node.name}") private val nodeName: String,
    @Value("\${distributedlock.node.url}") private val nodeUrl: String,
    @Value("\${distributedlock.node.electionTimeout}") private val electionTimeout: Long,
) : NodeStateRepository {

    private val log = LoggerFactory.getLogger(javaClass)
    private val state: AtomicReference<NodeInfo> = AtomicReference(
        NodeInfo(name = nodeName, url = nodeUrl)
    )

    override fun getState(): NodeInfo = state.get()

    override fun saveState(node: NodeInfo) {
        state.set(node)
    }

    override fun updateLastHeartbeat() {
        state.updateAndGet { it.copy(lastHeartbeatAt = Instant.now()) }
    }

    override fun isHeartbeatExpired(timeoutMs: Long): Boolean =
        Duration.between(state.get().lastHeartbeatAt, Instant.now()).toMillis() > timeoutMs

    fun isHeartbeatExpired(): Boolean = isHeartbeatExpired(electionTimeout)

    override fun isLeader(): Boolean = state.get().role == Role.LEADER

    override fun getLeaderUrl(): String? = state.get().leaderUrl

    fun becomeLeader() {
        val current = state.get()
        state.set(current.asLeader())
        MDC.put("node", nodeName); MDC.put("term", state.get().term.toString())
        log.info("Node transitioned to LEADER")
        MDC.remove("node"); MDC.remove("term")
    }

    fun becomeFollower(term: Long, leader: String?, url: String?) {
        val current = state.get()
        state.set(current.asFollower(term, leader, url))
        MDC.put("node", nodeName); MDC.put("term", term.toString())
        MDC.put("leader", leader); MDC.put("leaderUrl", url)
        log.info("Node transitioned to FOLLOWER")
        MDC.remove("node"); MDC.remove("term"); MDC.remove("leader"); MDC.remove("leaderUrl")
    }

    fun becomeCandidate() {
        val current = state.get()
        state.set(current.asCandidate())
        MDC.put("node", nodeName); MDC.put("term", state.get().term.toString())
        log.info("Node transitioned to CANDIDATE")
        MDC.remove("node"); MDC.remove("term")
    }

    fun voteFor(candidateId: String) {
        state.updateAndGet { it.withVote(candidateId) }
    }

    fun clearVotedFor() {
        state.updateAndGet { it.withoutVote() }
    }
}

