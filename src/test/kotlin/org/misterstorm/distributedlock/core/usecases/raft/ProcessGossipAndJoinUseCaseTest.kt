package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.GossipData
import org.misterstorm.distributedlock.core.models.raft.JoinInput
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ProcessGossipUseCaseTest {

    @Test
    fun `should merge incoming nodes into peer repository`() = runTest {
        val peerRepo = mockk<PeerRepository>(relaxed = true)
        every { peerRepo.getAllNodes() } returns mapOf("node1" to "http://node1:8080", "node2" to "http://node2:8081")
        val sut = ProcessGossipUseCase(peerRepo)

        val result = sut.execute(GossipData(nodes = mapOf("node2" to "http://node2:8081"), deadNodes = emptySet()))
        assertAll(
            { verify(exactly = 1) { peerRepo.merge(mapOf("node2" to "http://node2:8081")) } },
            { assertTrue(result.nodes.containsKey("node1")) },
        )
    }

    @Test
    fun `should apply dead nodes removals`() = runTest {
        val peerRepo = mockk<PeerRepository>(relaxed = true)
        every { peerRepo.getAllNodes() } returns emptyMap()
        val sut = ProcessGossipUseCase(peerRepo)

        sut.execute(GossipData(nodes = emptyMap(), deadNodes = setOf("http://dead:9090")))
        verify(exactly = 1) { peerRepo.applyRemovals(setOf("http://dead:9090")) }
    }

    @Test
    fun `should return all known nodes in response`() = runTest {
        val peerRepo = mockk<PeerRepository>(relaxed = true)
        every { peerRepo.getAllNodes() } returns mapOf("self" to "http://self:8080")
        val sut = ProcessGossipUseCase(peerRepo)
        val result = sut.execute(GossipData(emptyMap()))
        assertEquals(mapOf("self" to "http://self:8080"), result.nodes)
    }
}

class ProcessJoinUseCaseTest {

    @Test
    fun `should register joining node in peer repository`() = runTest {
        val peerRepo = mockk<PeerRepository>(relaxed = true)
        every { peerRepo.getAllNodes() } returns mapOf("node1" to "http://node1:8080", "node2" to "http://node2:8081")
        val sut = ProcessJoinUseCase(peerRepo)

        val result = sut.execute(JoinInput("node2", "http://node2:8081"))
        assertAll(
            { verify { peerRepo.merge(mapOf("node2" to "http://node2:8081")) } },
            { assertTrue(result.nodes.containsKey("node2")) },
        )
    }

    @Test
    fun `should return gossip with all known nodes`() = runTest {
        val peerRepo = mockk<PeerRepository>(relaxed = true)
        every { peerRepo.getAllNodes() } returns mapOf("self" to "http://self:8080", "joiner" to "http://joiner:9090")
        val sut = ProcessJoinUseCase(peerRepo)
        val result = sut.execute(JoinInput("joiner", "http://joiner:9090"))
        assertTrue(result.nodes.containsKey("self"))
        assertTrue(result.nodes.containsKey("joiner"))
    }
}

