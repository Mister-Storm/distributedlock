package org.misterstorm.distributedlock.core.adapter

interface PeerRepository {
    fun getSelfName(): String
    fun getSelfUrl(): String
    fun getPeerUrls(): List<String>
    fun getAllNodes(): Map<String, String>
    fun merge(nodes: Map<String, String>)
    fun remove(url: String)
    fun getPendingRemovals(): Set<String>
    fun clearPendingRemovals()
    fun applyRemovals(deadNodes: Set<String>)
}

