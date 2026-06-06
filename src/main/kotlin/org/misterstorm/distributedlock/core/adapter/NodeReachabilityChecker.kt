package org.misterstorm.distributedlock.core.adapter

interface NodeReachabilityChecker {
    fun isReachable(url: String): Boolean
}

