package org.misterstorm.distributedlock.core.adapter

interface LeaderStatus {
    fun isLeader(): Boolean
    fun getLeaderUrl(): String?
}