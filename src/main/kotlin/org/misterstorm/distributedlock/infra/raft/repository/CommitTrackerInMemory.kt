package org.misterstorm.distributedlock.infra.raft.repository

import org.misterstorm.distributedlock.core.adapter.CommitTracker
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentLinkedQueue

@Component
class CommitTrackerInMemory : CommitTracker {
    companion object {
        private const val MAX_RECENT_COMMITS = 50
    }

    private val recentCommits = ConcurrentLinkedQueue<String>()

    override fun recordCommit(key: String) {
        recentCommits.add(key)
        while (recentCommits.size > MAX_RECENT_COMMITS) {
            recentCommits.poll()
        }
    }

    override fun getRecentCommits(): List<String> = recentCommits.toList()
}

