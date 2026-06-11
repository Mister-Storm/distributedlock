package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.adapter.VoteRequester
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.misterstorm.distributedlock.core.models.raft.VoteOutput
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StartElectionUseCaseTest {

    private fun buildNodeInfo(
        name: String = "node1",
        url: String = "http://node1:8080",
        role: Role = Role.CANDIDATE,
        term: Long = 0L,
    ) = NodeInfo(name = name, url = url, role = role, term = term)

    private fun mockPeerRepository(
        peerRepository: PeerRepository,
        healthyPeers: List<String>,
    ) {
        every { peerRepository.getHealthyPeerUrls() } returns healthyPeers
        every { peerRepository.getRegisteredPeerUrls() } returns healthyPeers
        every { peerRepository.getUnhealthyPeerUrls() } returns emptyList()
    }

    private fun createSut(
        healthyPeers: List<String> = emptyList(),
        voteGranted: Boolean = true,
    ): Triple<StartElectionUseCase, NodeStateRepository, PeerRepository> {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()

        every { nodeStateRepository.getState() } returns buildNodeInfo()
        mockPeerRepository(peerRepository, healthyPeers)
        every { voteRequester.requestVote(any(), any()) } returns VoteOutput(1L, voteGranted)

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        return Triple(sut, nodeStateRepository, peerRepository)
    }

    @Test
    fun `should become leader when quorum is reached with single node`() = runTest {
        val (sut, nodeStateRepo, _) = createSut(healthyPeers = emptyList())
        val result = sut.execute(Unit)
        assertAll(
            { assertTrue(result.isRight()) },
            { assertTrue(result.getOrNull()!!.becameLeader) },
            { verify(atLeast = 2) { nodeStateRepo.saveState(any()) } },
        )
    }

    @Test
    fun `should become leader when all healthy peers grant vote`() = runTest {
        val (sut, _, _) = createSut(
            healthyPeers = listOf("http://node2:8081", "http://node3:8082"),
            voteGranted = true,
        )
        val result = sut.execute(Unit)
        assertTrue(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should not become leader when no healthy peer grants vote in 3-node cluster`() = runTest {
        val (sut, nodeStateRepo, _) = createSut(
            healthyPeers = listOf("http://node2:8081", "http://node3:8082"),
            voteGranted = false,
        )
        val result = sut.execute(Unit)
        assertAll(
            { assertFalse(result.getOrNull()!!.becameLeader) },
            { verify { nodeStateRepo.saveState(match { it.role == Role.FOLLOWER }) } },
        )
    }

    @Test
    fun `should become leader when majority of healthy peers grant vote`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        every { nodeStateRepository.getState() } returns buildNodeInfo()
        mockPeerRepository(peerRepository, listOf("http://node2:8081", "http://node3:8082"))
        every { voteRequester.requestVote("http://node2:8081", any()) } returns VoteOutput(1L, true)
        every { voteRequester.requestVote("http://node3:8082", any()) } returns VoteOutput(1L, false)

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        val result = sut.execute(Unit)
        assertTrue(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should become leader when registered peers are unhealthy leaving solo healthy cluster`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        every { nodeStateRepository.getState() } returns buildNodeInfo()
        every { peerRepository.getRegisteredPeerUrls() } returns listOf("http://node2:8081")
        every { peerRepository.getHealthyPeerUrls() } returns emptyList()
        every { peerRepository.getUnhealthyPeerUrls() } returns listOf("http://node2:8081")

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        val result = sut.execute(Unit)
        assertTrue(result.getOrNull()!!.becameLeader)
        verify(exactly = 0) { voteRequester.requestVote(any(), any()) }
    }

    @Test
    fun `should transition to candidate state before requesting votes`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        val savedStates = mutableListOf<NodeInfo>()

        every { nodeStateRepository.getState() } returns buildNodeInfo()
        mockPeerRepository(peerRepository, emptyList())
        every { nodeStateRepository.saveState(capture(savedStates)) } returns Unit

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        sut.execute(Unit)

        assertTrue(savedStates.first().role == Role.CANDIDATE)
    }
}
