package org.misterstorm.distributedlock.core.adapter

import org.misterstorm.distributedlock.core.models.raft.PeerEntry

interface PeerRepository {
    fun getSelfName(): String
    fun getSelfUrl(): String
    fun getRegisteredPeerUrls(): List<String>
    fun getHealthyPeerUrls(): List<String>
    fun getUnhealthyPeerUrls(): List<String>
    fun getPeerEntries(): List<PeerEntry>
    fun getAllNodes(): Map<String, String>
    fun merge(nodes: Map<String, String>)
    fun markUnhealthy(url: String)
    fun markHealthy(url: String)
    fun remove(url: String)
}
