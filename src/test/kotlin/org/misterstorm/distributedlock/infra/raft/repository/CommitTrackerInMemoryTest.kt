package org.misterstorm.distributedlock.infra.raft.repository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CommitTrackerInMemoryTest {

    @Test
    fun `should record and retrieve commits`() {
        val tracker = CommitTrackerInMemory()
        tracker.recordCommit("key-1")
        tracker.recordCommit("key-2")
        assertTrue(tracker.getRecentCommits().contains("key-1"))
        assertTrue(tracker.getRecentCommits().contains("key-2"))
    }

    @Test
    fun `should start empty`() {
        val tracker = CommitTrackerInMemory()
        assertTrue(tracker.getRecentCommits().isEmpty())
    }

    @Test
    fun `should evict oldest entries when max capacity exceeded`() {
        val tracker = CommitTrackerInMemory()
        repeat(55) { i -> tracker.recordCommit("key-$i") }
        val commits = tracker.getRecentCommits()
        assertEquals(50, commits.size)
        assertFalse(commits.contains("key-0"))
        assertTrue(commits.contains("key-54"))
    }

    @Test
    fun `should keep at most 50 commits`() {
        val tracker = CommitTrackerInMemory()
        repeat(60) { tracker.recordCommit("k-$it") }
        assertEquals(50, tracker.getRecentCommits().size)
    }
}

