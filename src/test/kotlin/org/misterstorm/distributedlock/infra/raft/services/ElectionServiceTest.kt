package org.misterstorm.distributedlock.infra.raft.services

import arrow.core.right
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.raft.ElectionOutput
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.misterstorm.distributedlock.core.usecases.raft.StartElectionUseCase
import org.misterstorm.distributedlock.infra.raft.repository.NodeStateRepositoryInMemory

class ElectionServiceTest {

    private fun createSut(
        role: Role = Role.FOLLOWER,
        heartbeatExpired: Boolean = true,
    ): Pair<ElectionService, StartElectionUseCase> {
        val nodeStateRepo = mockk<NodeStateRepositoryInMemory>()
        val startElectionUseCase = mockk<StartElectionUseCase>()
        every { nodeStateRepo.getState() } returns NodeInfo(name = "node1", url = "http://node1:8080", role = role)
        every { nodeStateRepo.isHeartbeatExpired(any()) } returns heartbeatExpired
        coEvery { startElectionUseCase.execute(Unit) } returns ElectionOutput(becameLeader = true).right()
        return ElectionService(nodeStateRepo, startElectionUseCase, 6000L) to startElectionUseCase
    }

    @Test
    fun `should not start election when node is already leader`() {
        val (sut, useCase) = createSut(role = Role.LEADER, heartbeatExpired = true)
        sut.checkElectionTimeout()
        coVerify(exactly = 0) { useCase.execute(Unit) }
    }

    @Test
    fun `should not start election when heartbeat has not expired`() {
        val (sut, useCase) = createSut(role = Role.FOLLOWER, heartbeatExpired = false)
        sut.checkElectionTimeout()
        coVerify(exactly = 0) { useCase.execute(Unit) }
    }

    @Test
    fun `should start election when not leader and heartbeat expired`() {
        val (sut, useCase) = createSut(role = Role.FOLLOWER, heartbeatExpired = true)
        sut.checkElectionTimeout()
        coVerify(exactly = 1) { useCase.execute(Unit) }
    }

    @Test
    fun `should start election when stuck as candidate with expired heartbeat`() {
        val (sut, useCase) = createSut(role = Role.CANDIDATE, heartbeatExpired = true)
        sut.checkElectionTimeout()
        coVerify(exactly = 1) { useCase.execute(Unit) }
    }

    @Test
    fun `startElection should delegate to use case`() {
        val (sut, useCase) = createSut()
        sut.startElection()
        coVerify(exactly = 1) { useCase.execute(Unit) }
    }
}
