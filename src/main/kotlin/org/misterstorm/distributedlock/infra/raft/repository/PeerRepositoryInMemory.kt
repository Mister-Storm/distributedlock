package org.misterstorm.distributedlock.infra.raft.repository

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.PeerEntry
import org.misterstorm.distributedlock.core.models.raft.PeerReachability
import org.misterstorm.distributedlock.infra.raft.models.RaftProperties
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

@Component
class PeerRepositoryInMemory(
    @Value("\${distributedlock.node.name}") private val selfName: String,
    @Value("\${distributedlock.node.url}") private val selfUrl: String,
    raftProperties: RaftProperties,
    private val peers: ConcurrentHashMap<String, PeerEntry> = ConcurrentHashMap(),
) : PeerRepository {

    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        const val MAX_FAILURES_BEFORE_UNHEALTHY = 3
    }

    init {
        raftProperties.seeds.forEachIndexed { index, url ->
            if (url != selfUrl) {
                peers["seed-$index"] = PeerEntry(name = "seed-$index", url = url)
            }
        }
    }

    override fun getSelfName(): String = selfName
    override fun getSelfUrl(): String = selfUrl

    override fun getRegisteredPeerUrls(): List<String> = peers.values.map { it.url }

    override fun getHealthyPeerUrls(): List<String> =
        peers.values
            .filter { it.reachability == PeerReachability.HEALTHY }
            .map { it.url }

    override fun getUnhealthyPeerUrls(): List<String> =
        peers.values
            .filter { it.reachability == PeerReachability.UNHEALTHY }
            .map { it.url }

    override fun getPeerEntries(): List<PeerEntry> = peers.values.toList()

    override fun getAllNodes(): Map<String, String> =
        peers.mapValues { it.value.url } + mapOf(selfName to selfUrl)

    override fun merge(nodes: Map<String, String>) = nodes.forEach { (name, url) ->
        if (name != selfName) {
            peers.entries.removeIf { it.value.url == url && it.key.startsWith("seed-") }
            val existing = peers[name]
            peers[name] = PeerEntry(
                name = name,
                url = url,
                reachability = existing?.reachability ?: PeerReachability.HEALTHY,
                consecutiveFailures = existing?.consecutiveFailures ?: 0,
                lastSeenAt = existing?.lastSeenAt ?: Instant.now(),
                lastHealthCheckAt = existing?.lastHealthCheckAt,
            )
        }
    }

    override fun markUnhealthy(url: String) {
        if (url == selfUrl) return
        peers.entries
            .filter { it.value.url == url }
            .forEach { (key, entry) ->
                val failures = entry.consecutiveFailures + 1
                val nowUnhealthy = failures >= MAX_FAILURES_BEFORE_UNHEALTHY
                val newReachability = if (nowUnhealthy) {
                    PeerReachability.UNHEALTHY
                } else {
                    entry.reachability
                }
                if (entry.reachability == PeerReachability.HEALTHY && newReachability == PeerReachability.UNHEALTHY) {
                    logPeerTransition(url, "UNHEALTHY", failures)
                }
                peers[key] = entry.copy(
                    consecutiveFailures = failures,
                    reachability = newReachability,
                    lastHealthCheckAt = Instant.now(),
                )
            }
    }

    override fun markHealthy(url: String) {
        peers.entries
            .filter { it.value.url == url }
            .forEach { (key, entry) ->
                if (entry.reachability == PeerReachability.UNHEALTHY) {
                    logPeerTransition(url, "HEALTHY", 0)
                }
                peers[key] = entry.copy(
                    reachability = PeerReachability.HEALTHY,
                    consecutiveFailures = 0,
                    lastSeenAt = Instant.now(),
                    lastHealthCheckAt = Instant.now(),
                )
            }
    }

    override fun remove(url: String) {
        peers.entries.removeIf { it.value.url == url }
    }

    private fun logPeerTransition(url: String, status: String, failures: Int) {
        MDC.put("peer", url)
        MDC.put("peerStatus", status)
        MDC.put("peerFailures", failures.toString())
        MDC.put("event", "peer_health_transition")
        log.info("Peer health status changed")
        MDC.remove("peer")
        MDC.remove("peerStatus")
        MDC.remove("peerFailures")
        MDC.remove("event")
    }
}
