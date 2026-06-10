package org.misterstorm.distributedlock.core.adapter

import org.misterstorm.distributedlock.core.models.raft.PeerEntry

interface PeerRepository {
    fun getSelfName(): String
    fun getSelfUrl(): String
    fun getPeerUrls(): List<String>
    fun getReachablePeerUrls(): List<String>
    fun getPeerEntries(): List<PeerEntry>
    fun getAllNodes(): Map<String, String>
    fun merge(nodes: Map<String, String>)
    fun markUnreachable(url: String)
    fun markReachable(url: String)
    fun remove(url: String)
}
