package org.misterstorm.distributedlock.core.adapter

interface CommitTracker {
    fun recordCommit(key: String)
    fun getRecentCommits(): List<String>
}

