package org.misterstorm.distributedlock.core.models.raft

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.Role

class NodeInfoTest {

    private fun baseNode() = NodeInfo(name = "node1", url = "http://node1:8080")

    @Test
    fun `asLeader should set role LEADER and leader fields to self`() {
        val node = baseNode().asLeader()
        assertEquals(Role.LEADER, node.role)
        assertEquals("node1", node.leaderName)
        assertEquals("http://node1:8080", node.leaderUrl)
        assertTrue(node.isLeader())
    }

    @Test
    fun `asFollower should set role FOLLOWER, update term and leader info, clear votedFor`() {
        val node = baseNode().withVote("nodeX").asFollower(5L, "node2", "http://node2:8081")
        assertEquals(Role.FOLLOWER, node.role)
        assertEquals(5L, node.term)
        assertEquals("node2", node.leaderName)
        assertEquals("http://node2:8081", node.leaderUrl)
        assertNull(node.votedFor)
    }

    @Test
    fun `asCandidate should increment term, vote for self, clear leader`() {
        val node = baseNode().copy(term = 3L).asCandidate()
        assertEquals(Role.CANDIDATE, node.role)
        assertEquals(4L, node.term)
        assertEquals("node1", node.votedFor)
        assertNull(node.leaderName)
        assertNull(node.leaderUrl)
    }

    @Test
    fun `withVote should set votedFor`() {
        val node = baseNode().withVote("nodeZ")
        assertEquals("nodeZ", node.votedFor)
    }

    @Test
    fun `withoutVote should clear votedFor`() {
        val node = baseNode().withVote("nodeZ").withoutVote()
        assertNull(node.votedFor)
    }

    @Test
    fun `isLeader should return false for non-LEADER roles`() {
        assertTrue(!baseNode().copy(role = Role.FOLLOWER).isLeader())
        assertTrue(!baseNode().copy(role = Role.CANDIDATE).isLeader())
    }
}

