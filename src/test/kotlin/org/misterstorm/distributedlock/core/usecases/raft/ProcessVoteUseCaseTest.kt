package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.misterstorm.distributedlock.core.models.raft.VoteInput
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProcessVoteUseCaseTest {

    private fun buildNodeInfo(
        name: String = "node1",
        term: Long = 0L,
        votedFor: String? = null,
    ) = NodeInfo(name = name, url = "http://node1:8080", term = term, votedFor = votedFor)

    private fun createSut(nodeInfo: NodeInfo): Triple<ProcessVoteUseCase, NodeStateRepository, PeerRepository> {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns nodeInfo
        return Triple(ProcessVoteUseCase(nodeStateRepo, peerRepository), nodeStateRepo, peerRepository)
    }

    @Test
    fun `should grant vote when node has not voted`() = runTest {
        val (sut, _, _) = createSut(buildNodeInfo(votedFor = null))
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 1L))
        assertAll(
            { assertTrue(result.isRight()) },
            { assertTrue(result.getOrNull()!!.voteGranted) },
        )
    }

    @Test
    fun `should grant vote when already voted for same candidate`() = runTest {
        val (sut, _, _) = createSut(buildNodeInfo(votedFor = "nodeA"))
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 1L))
        assertTrue(result.getOrNull()!!.voteGranted)
    }

    @Test
    fun `should deny vote when already voted for different candidate`() = runTest {
        val (sut, _, _) = createSut(buildNodeInfo(votedFor = "nodeB"))
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 1L))
        assertFalse(result.getOrNull()!!.voteGranted)
    }

    @Test
    fun `should deny vote when request term is stale`() = runTest {
        val (sut, _, _) = createSut(buildNodeInfo(term = 10L))
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 5L))
        assertAll(
            { assertTrue(result.isRight()) },
            { assertFalse(result.getOrNull()!!.voteGranted) },
            { assertEquals(10L, result.getOrNull()!!.term) },
        )
    }

    @Test
    fun `should merge candidate into peer repository on vote grant`() = runTest {
        val (sut, _, peerRepo) = createSut(buildNodeInfo())
        sut.execute(VoteInput("nodeA", "http://nodeA:8080", 1L))
        verify(exactly = 1) { peerRepo.merge(mapOf("nodeA" to "http://nodeA:8080")) }
    }

    @Test
    fun `should not merge peer when vote is denied due to stale term`() = runTest {
        val (sut, _, peerRepo) = createSut(buildNodeInfo(term = 10L))
        sut.execute(VoteInput("nodeA", "http://nodeA:8080", 5L))
        verify(exactly = 0) { peerRepo.merge(any()) }
    }

    @Test
    fun `should record vote for the candidate`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo(name = "node1")
        val savedStates = mutableListOf<NodeInfo>()
        every { nodeStateRepo.saveState(capture(savedStates)) } returns Unit
        val sut = ProcessVoteUseCase(nodeStateRepo, peerRepository)
        sut.execute(VoteInput("nodeZ", "http://nodeZ:8080", 1L))
        assertEquals("nodeZ", savedStates.last().votedFor)
    }

    @Test
    fun `should record vote for candidate even when candidate name is lexicographically smaller than self`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>(relaxed = true)
        // self = "nodeZ" > candidate = "nodeA" — must still vote for the candidate
        every { nodeStateRepo.getState() } returns buildNodeInfo(name = "nodeZ")
        val savedStates = mutableListOf<NodeInfo>()
        every { nodeStateRepo.saveState(capture(savedStates)) } returns Unit
        val sut = ProcessVoteUseCase(nodeStateRepo, peerRepository)
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 1L))
        assertTrue(result.getOrNull()!!.voteGranted)
        assertEquals("nodeA", savedStates.last().votedFor)
    }

    @Test
    fun `should return current term in response on stale request`() = runTest {
        val (sut, _, _) = createSut(buildNodeInfo(term = 7L))
        val result = sut.execute(VoteInput("nodeA", "http://nodeA:8080", 3L))
        assertEquals(7L, result.getOrNull()!!.term)
    }
}

