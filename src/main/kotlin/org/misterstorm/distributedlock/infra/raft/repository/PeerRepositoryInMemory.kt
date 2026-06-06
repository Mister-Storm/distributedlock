package org.misterstorm.distributedlock.infra.raft.repository

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.infra.raft.models.RaftProperties
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class PeerRepositoryInMemory(
    @Value("\${distributedlock.node.name}") private val selfName: String,
    @Value("\${distributedlock.node.url}") private val selfUrl: String,
    raftProperties: RaftProperties,
    private val peers: ConcurrentHashMap<String, String> = ConcurrentHashMap(),
) : PeerRepository {

    private val pendingRemovals: MutableSet<String> = ConcurrentHashMap.newKeySet()

    init {
        raftProperties.seeds.forEachIndexed { index, url ->
            if (url != selfUrl) {
                peers["seed-$index"] = url
            }
        }
    }

    override fun getSelfName(): String = selfName
    override fun getSelfUrl(): String = selfUrl
    override fun getPeerUrls(): List<String> = peers.values.toList()
    override fun getAllNodes(): Map<String, String> = peers + mapOf(selfName to selfUrl)

    override fun merge(nodes: Map<String, String>) = nodes.forEach { (name, url) ->
        if (name != selfName) {
            peers.entries.removeIf { it.value == url && it.key.startsWith("seed-") }
            peers[name] = url
        }
    }

    override fun remove(url: String) {
        peers.entries.removeIf { it.value == url }
        if (url != selfUrl) pendingRemovals.add(url)
    }

    override fun getPendingRemovals(): Set<String> = pendingRemovals.toSet()
    override fun clearPendingRemovals() = pendingRemovals.clear()

    override fun applyRemovals(deadNodes: Set<String>) = deadNodes.forEach { url ->
        peers.entries.removeIf { it.value == url }
    }
}

