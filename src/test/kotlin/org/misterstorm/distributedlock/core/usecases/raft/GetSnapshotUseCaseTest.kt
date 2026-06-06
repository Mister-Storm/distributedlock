package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.raft.NodeInfo
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import kotlin.test.assertTrue

class GetSnapshotUseCaseTest {

    private fun buildNodeInfo(role: Role) = NodeInfo(name = "node1", url = "http://node1:8080", role = role)

    @Test
    fun `should return snapshot when leader`() = runTest {
        val lock = createLock()
        val nodeStateRepo = mockk<NodeStateRepository>()
        every { nodeStateRepo.getState() } returns buildNodeInfo(Role.LEADER)
        every { nodeStateRepo.isLeader() } returns true
        val lockRepo = object : TestLockRepository() {
            override fun getAllLocks() = listOf(lock)
            override fun getAllInQueue() = emptyList<org.misterstorm.distributedlock.core.models.lock.Lock>()
        }
        val sut = GetSnapshotUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(Unit)
        assertAll(
            { assertTrue(result.isRight()) },
            { assertTrue(result.getOrNull()!!.locks.isNotEmpty()) },
        )
    }

    @Test
    fun `should return NotLeader error when not leader`() = runTest {
        val nodeStateRepo = mockk<NodeStateRepository>()
        every { nodeStateRepo.getState() } returns buildNodeInfo(Role.FOLLOWER)
        every { nodeStateRepo.isLeader() } returns false
        val lockRepo = object : TestLockRepository() {}
        val sut = GetSnapshotUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(Unit)
        assertAll(
            { assertTrue(result.isLeft()) },
            { assertTrue(result.leftOrNull() is BusinessError.NotLeader) },
        )
    }

    @Test
    fun `should exclude expired locks from snapshot`() = runTest {
        val activeLock = createLock(key = "active")
        val expiredLock = createLock(key = "expired", expirationTime = java.time.LocalDateTime.now().minusSeconds(10))
        val nodeStateRepo = mockk<NodeStateRepository>()
        every { nodeStateRepo.getState() } returns buildNodeInfo(Role.LEADER)
        every { nodeStateRepo.isLeader() } returns true
        val lockRepo = object : TestLockRepository() {
            override fun getAllLocks() = listOf(activeLock, expiredLock)
            override fun getAllInQueue() = emptyList<org.misterstorm.distributedlock.core.models.lock.Lock>()
        }
        val sut = GetSnapshotUseCase(nodeStateRepo, lockRepo)
        val result = sut.execute(Unit)
        val locks = result.getOrNull()!!.locks
        assertTrue(locks.any { (it as org.misterstorm.distributedlock.core.models.lock.Lock).key == "active" })
        assertTrue(locks.none { (it as org.misterstorm.distributedlock.core.models.lock.Lock).key == "expired" })
    }
}

