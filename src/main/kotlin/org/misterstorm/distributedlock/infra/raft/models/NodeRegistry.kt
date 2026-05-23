package org.misterstorm.distributedlock.infra.raft.models

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class NodeRegistry(
    @Value("\${distributedlock.node.name}") val selfName: String,
    @Value("\${distributedlock.node.url}") val selfUrl: String,
    raftProperties: RaftProperties,
    private val peers: ConcurrentHashMap<String, String> = ConcurrentHashMap<String, String>()
) {
    private val pendingRemovals: MutableSet<String> = ConcurrentHashMap.newKeySet()

    init {
        raftProperties.seeds.forEachIndexed { index, url ->
            if (url != selfUrl) {
                peers["seed-$index"] = url
            }
        }
    }

    fun getPeerUrls(): List<String> = peers.values.toList()
    fun getAllNodes(): Map<String, String> = peers + mapOf(selfName to selfUrl)

    fun merge(nodes: Map<String, String>) = nodes.forEach { (name, url) ->
        if (name != selfName) {
            peers.entries.removeIf { it.value == url && it.key.startsWith("seed-") }
            peers[name] = url
        }
    }

    fun remove(nodeUrl: String) {
        peers.entries.removeIf { it.value == nodeUrl }
        if (nodeUrl != selfUrl) pendingRemovals.add(nodeUrl)
    }

    fun getPendingRemovals(): Set<String> = pendingRemovals.toSet()

    fun clearPendingRemovals() = pendingRemovals.clear()

    fun applyRemovals(deadNodes: Set<String>) = deadNodes.forEach { url ->
        peers.entries.removeIf { it.value == url }
    }
}