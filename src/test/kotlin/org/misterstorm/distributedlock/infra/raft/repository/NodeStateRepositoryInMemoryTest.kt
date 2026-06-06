package org.misterstorm.distributedlock.infra.raft.repository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.Role

class NodeStateRepositoryInMemoryTest {

    private lateinit var sut: NodeStateRepositoryInMemory

    @BeforeEach
    fun setup() {
        sut = NodeStateRepositoryInMemory(
            nodeName = "node1",
            nodeUrl = "http://localhost:8080",
            electionTimeout = 5000L,
        )
    }

    @Test
    fun `should start as CANDIDATE with term 0`() {
        val state = sut.getState()
        assertEquals(Role.CANDIDATE, state.role)
        assertEquals(0L, state.term)
        assertNull(state.leaderName)
        assertNull(state.leaderUrl)
        assertNull(state.votedFor)
    }

    @Test
    fun `should transition to LEADER via becomeLeader`() {
        sut.becomeLeader()
        val state = sut.getState()
        assertEquals(Role.LEADER, state.role)
        assertEquals("node1", state.leaderName)
        assertEquals("http://localhost:8080", state.leaderUrl)
        assertTrue(sut.isLeader())
    }

    @Test
    fun `should transition to FOLLOWER via becomeFollower`() {
        sut.becomeFollower(5L, "node2", "http://node2:8081")
        val state = sut.getState()
        assertEquals(Role.FOLLOWER, state.role)
        assertEquals(5L, state.term)
        assertEquals("node2", state.leaderName)
        assertEquals("http://node2:8081", state.leaderUrl)
        assertNull(state.votedFor)
        assertFalse(sut.isLeader())
    }

    @Test
    fun `should transition to CANDIDATE via becomeCandidate`() {
        sut.becomeFollower(3L, "node2", "http://node2:8081")
        sut.becomeCandidate()
        val state = sut.getState()
        assertEquals(Role.CANDIDATE, state.role)
        assertEquals(4L, state.term) // term incremented
        assertEquals("node1", state.votedFor) // votes for self
        assertNull(state.leaderName)
    }

    @Test
    fun `should record vote via voteFor`() {
        sut.voteFor("node2")
        assertEquals("node2", sut.getState().votedFor)
    }

    @Test
    fun `should clear vote via clearVotedFor`() {
        sut.voteFor("node2")
        sut.clearVotedFor()
        assertNull(sut.getState().votedFor)
    }

    @Test
    fun `should return leader url`() {
        assertNull(sut.getLeaderUrl())
        sut.becomeFollower(1L, "node2", "http://node2:8081")
        assertEquals("http://node2:8081", sut.getLeaderUrl())
    }

    @Test
    fun `should detect expired heartbeat`() {
        // Node with very small timeout
        val fastExpiry = NodeStateRepositoryInMemory("n", "u", 1L)
        Thread.sleep(50)
        assertTrue(fastExpiry.isHeartbeatExpired(1L))
    }

    @Test
    fun `should not detect expired heartbeat when within timeout`() {
        assertFalse(sut.isHeartbeatExpired(5000L))
    }

    @Test
    fun `should reset heartbeat timer via updateLastHeartbeat`() {
        val fastExpiry = NodeStateRepositoryInMemory("n", "u", 1L)
        Thread.sleep(50)
        fastExpiry.updateLastHeartbeat()
        assertFalse(fastExpiry.isHeartbeatExpired(1000L))
    }

    @Test
    fun `should persist saveState correctly`() {
        val original = sut.getState()
        val modified = original.copy(term = 99L)
        sut.saveState(modified)
        assertEquals(99L, sut.getState().term)
    }

    @Test
    fun `becomeFollower should accept null leaderName and leaderUrl`() {
        sut.becomeFollower(1L, null, null)
        val state = sut.getState()
        assertNull(state.leaderName)
        assertNull(state.leaderUrl)
        assertEquals(Role.FOLLOWER, state.role)
    }
}

