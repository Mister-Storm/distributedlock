package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
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

    private fun createSut(
        peers: List<String> = emptyList(),
        voteGranted: Boolean = true,
    ): Triple<StartElectionUseCase, NodeStateRepository, PeerRepository> {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()

        every { nodeStateRepository.getState() } returns buildNodeInfo()
        every { peerRepository.getPeerUrls() } returns peers
        every { voteRequester.requestVote(any(), any()) } returns VoteOutput(1L, voteGranted)

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        return Triple(sut, nodeStateRepository, peerRepository)
    }

    @Test
    fun `should become leader when quorum is reached with single node`() = runTest {
        val (sut, nodeStateRepo, _) = createSut(peers = emptyList())
        val result = sut.execute(Unit)
        assertAll(
            { assertTrue(result.isRight()) },
            { assertTrue(result.getOrNull()!!.becameLeader) },
            // saveState called twice: once for candidate, once for leader
            { verify(atLeast = 2) { nodeStateRepo.saveState(any()) } },
        )
    }

    @Test
    fun `should become leader when all peers grant vote`() = runTest {
        val (sut, _, _) = createSut(peers = listOf("http://node2:8081", "http://node3:8082"), voteGranted = true)
        val result = sut.execute(Unit)
        assertTrue(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should not become leader when no peer grants vote in 3-node cluster`() = runTest {
        val (sut, _, _) = createSut(peers = listOf("http://node2:8081", "http://node3:8082"), voteGranted = false)
        val result = sut.execute(Unit)
        assertFalse(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should become leader when majority grants vote in 3-node cluster`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        every { nodeStateRepository.getState() } returns buildNodeInfo()
        every { peerRepository.getPeerUrls() } returns listOf("http://node2:8081", "http://node3:8082")
        every { voteRequester.requestVote("http://node2:8081", any()) } returns VoteOutput(1L, true)
        every { voteRequester.requestVote("http://node3:8082", any()) } returns VoteOutput(1L, false)

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        val result = sut.execute(Unit)
        assertTrue(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should treat unreachable peer as no vote`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        every { nodeStateRepository.getState() } returns buildNodeInfo()
        every { peerRepository.getPeerUrls() } returns listOf("http://node2:8081")
        every { voteRequester.requestVote(any(), any()) } returns null

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        val result = sut.execute(Unit)
        // 1 vote (self only) out of 2 nodes — quorum is 2 — should NOT win
        assertFalse(result.getOrNull()!!.becameLeader)
    }

    @Test
    fun `should transition to candidate state before requesting votes`() = runTest {
        val nodeStateRepository = mockk<NodeStateRepository>(relaxed = true)
        val peerRepository = mockk<PeerRepository>()
        val voteRequester = mockk<VoteRequester>()
        val savedStates = mutableListOf<NodeInfo>()

        every { nodeStateRepository.getState() } returns buildNodeInfo()
        every { peerRepository.getPeerUrls() } returns emptyList()
        every { nodeStateRepository.saveState(capture(savedStates)) } returns Unit

        val sut = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)
        sut.execute(Unit)

        // First save must be the candidate state
        assertTrue(savedStates.first().role == Role.CANDIDATE)
    }
}

