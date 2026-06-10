package org.misterstorm.distributedlock.infra.raft.repository

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.PeerEntry
import org.misterstorm.distributedlock.core.models.raft.PeerReachability
import org.misterstorm.distributedlock.infra.raft.models.RaftProperties
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

    companion object {
        const val MAX_FAILURES_BEFORE_UNREACHABLE = 3
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

    override fun getPeerUrls(): List<String> = peers.values.map { it.url }

    override fun getReachablePeerUrls(): List<String> =
        peers.values
            .filter { it.reachability == PeerReachability.REACHABLE }
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
                reachability = existing?.reachability ?: PeerReachability.REACHABLE,
                consecutiveFailures = existing?.consecutiveFailures ?: 0,
                lastSeenAt = existing?.lastSeenAt ?: Instant.now(),
            )
        }
    }

    override fun markUnreachable(url: String) {
        if (url == selfUrl) return
        peers.entries
            .filter { it.value.url == url }
            .forEach { (key, entry) ->
                val failures = entry.consecutiveFailures + 1
                peers[key] = entry.copy(
                    consecutiveFailures = failures,
                    reachability = if (failures >= MAX_FAILURES_BEFORE_UNREACHABLE) {
                        PeerReachability.UNREACHABLE
                    } else {
                        entry.reachability
                    },
                )
            }
    }

    override fun markReachable(url: String) {
        peers.entries
            .filter { it.value.url == url }
            .forEach { (key, entry) ->
                peers[key] = entry.copy(
                    reachability = PeerReachability.REACHABLE,
                    consecutiveFailures = 0,
                    lastSeenAt = Instant.now(),
                )
            }
    }

    override fun remove(url: String) {
        peers.entries.removeIf { it.value.url == url }
    }
}
