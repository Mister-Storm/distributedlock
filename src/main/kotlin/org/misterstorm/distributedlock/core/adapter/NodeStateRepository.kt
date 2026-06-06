package org.misterstorm.distributedlock.core.adapter

import org.misterstorm.distributedlock.core.models.raft.NodeInfo

interface NodeStateRepository : LeaderStatus {
    fun getState(): NodeInfo
    fun saveState(node: NodeInfo)
    fun updateLastHeartbeat()
    fun isHeartbeatExpired(timeoutMs: Long): Boolean
}

