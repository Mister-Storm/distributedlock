package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.raft.HeartbeatInput
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import kotlin.test.assertTrue

class ProcessHeartbeatUseCaseTest {

    private fun buildNodeInfo(role: Role = Role.CANDIDATE, term: Long = 0L) =
        NodeInfo(name = "node1", url = "http://node1:8080", role = role, term = term)

    @Test
    fun `should accept heartbeat and become follower`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo()
        val lockRepo = object : TestLockRepository() {
            override fun hasPending(idempotencyKey: String) = false
        }
        val sut = ProcessHeartbeatUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(HeartbeatInput("leader", "http://leader:8080", 5L))
        assertAll(
            { assertTrue(result.isRight()) },
            { verify(exactly = 1) { nodeStateRepo.saveState(any()) } },
        )
    }

    @Test
    fun `should reject heartbeat with stale term`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo(term = 10L)
        val lockRepo = object : TestLockRepository() {}
        val sut = ProcessHeartbeatUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(HeartbeatInput("old-leader", "http://old:8080", 5L))
        assertAll(
            { assertTrue(result.isLeft()) },
            { assertTrue(result.leftOrNull() is BusinessError.StaleTerm) },
        )
    }

    @Test
    fun `should reject heartbeat when node is already leader`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo(role = Role.LEADER, term = 1L)
        every { nodeStateRepo.isLeader() } returns true
        val lockRepo = object : TestLockRepository() {}
        val sut = ProcessHeartbeatUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(HeartbeatInput("other-leader", "http://other:8080", 5L))
        assertAll(
            { assertTrue(result.isLeft()) },
            { assertTrue(result.leftOrNull() is BusinessError.AlreadyLeader) },
        )
    }

    @Test
    fun `should commit pending entries from recentCommits`() = runTest {
        val key = "key-123"
        val committed = mutableListOf<String>()
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo()
        val lockRepo = object : TestLockRepository() {
            override fun hasPending(idempotencyKey: String) = idempotencyKey == key
            override fun commit(idempotencyKey: String) { committed.add(idempotencyKey) }
        }
        val sut = ProcessHeartbeatUseCase(nodeStateRepo, lockRepo)
        sut.execute(HeartbeatInput("leader", "http://leader:8080", 1L, recentCommits = listOf(key)))
        assertTrue(committed.contains(key))
    }

    @Test
    fun `should ignore unknown keys in recentCommits`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>(relaxed = true)
        every { nodeStateRepo.getState() } returns buildNodeInfo()
        val committed = mutableListOf<String>()
        val lockRepo = object : TestLockRepository() {
            override fun hasPending(idempotencyKey: String) = false
            override fun commit(idempotencyKey: String) { committed.add(idempotencyKey) }
        }
        val sut = ProcessHeartbeatUseCase(nodeStateRepo, lockRepo)
        sut.execute(HeartbeatInput("leader", "http://leader:8080", 1L, recentCommits = listOf("unknown-key")))
        assertTrue(committed.isEmpty())
    }
}

