package org.misterstorm.distributedlock.infra.raft.services

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createNodeState
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import java.time.LocalDateTime

class ExpiredLockCleanupServiceTest {

    private fun createExpiredLock(key: String = "test_key") = createLock(
        key = key,
        expirationTime = LocalDateTime.now().minusSeconds(10),
    )

    private fun createFollowerNodeState(): NodeState {
        val nodeState = NodeState(
            nodeName = "node2",
            nodeUrl = "http://localhost:8081",
            electionTimeout = 1000L,
        )
        nodeState.becomeFollower(1L, "node1", "http://localhost:8080")
        return nodeState
    }

    @Test
    fun `should do nothing when node is not the leader`() {
        val lockRepository = spyk(object : TestLockRepository() {})
        val nodeState = createFollowerNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 0) { lockRepository.getAllLocks() } },
            { verify(exactly = 0) { raftReplicationService.replicate(any(), any()) } },
        )
    }

    @Test
    fun `should do nothing when there are no expired locks`() {
        val activeLock = createLock()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(activeLock)
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { lockRepository.getAllLocks() } },
            { verify(exactly = 0) { raftReplicationService.replicate(any(), any()) } },
        )
    }

    @Test
    fun `should replicate RELEASE for expired lock with no queued candidate`() {
        val expiredLock = createExpiredLock()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(expiredLock)
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } returns true

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } },
            { verify(exactly = 0) { raftReplicationService.replicate(LockOperation.CREATE, any()) } },
            { verify(exactly = 0) { lockRepository.dequeue(any()) } },
        )
    }

    @Test
    fun `should replicate RELEASE and then CREATE when there is a queued candidate`() {
        val expiredLock = createExpiredLock()
        val promotedLock = createLock()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(expiredLock)
            override fun hasKeyInQueue(key: String): Boolean = true
            override fun dequeue(key: String): Lock = promotedLock
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } returns true
        every { raftReplicationService.replicate(LockOperation.CREATE, promotedLock) } returns true

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } },
            { verify(exactly = 1) { lockRepository.dequeue(expiredLock.key) } },
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.CREATE, promotedLock) } },
        )
    }

    @Test
    fun `should skip promotion when RELEASE quorum fails`() {
        val expiredLock = createExpiredLock()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(expiredLock)
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } returns false

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } },
            { verify(exactly = 0) { lockRepository.hasKeyInQueue(any()) } },
            { verify(exactly = 0) { lockRepository.dequeue(any()) } },
            { verify(exactly = 0) { raftReplicationService.replicate(LockOperation.CREATE, any()) } },
        )
    }

    @Test
    fun `should re-enqueue promoted lock when CREATE quorum fails`() {
        val expiredLock = createExpiredLock()
        val promotedLock = createLock()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(expiredLock)
            override fun hasKeyInQueue(key: String): Boolean = true
            override fun dequeue(key: String): Lock = promotedLock
            override fun addQueue(lock: Lock): Boolean = true
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } returns true
        every { raftReplicationService.replicate(LockOperation.CREATE, promotedLock) } returns false

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock) } },
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.CREATE, promotedLock) } },
            { verify(exactly = 1) { lockRepository.addQueue(promotedLock) } },
        )
    }

    @Test
    fun `should process multiple expired locks independently`() {
        val expiredLock1 = createExpiredLock("key_1")
        val expiredLock2 = createExpiredLock("key_2")
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getAllLocks(): Collection<Lock> = listOf(expiredLock1, expiredLock2)
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(LockOperation.RELEASE, any()) } returns true

        val sut = ExpiredLockCleanupService(lockRepository, nodeState, raftReplicationService)
        sut.cleanupExpiredLocks()

        assertAll(
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock1) } },
            { verify(exactly = 1) { raftReplicationService.replicate(LockOperation.RELEASE, expiredLock2) } },
        )
    }
}

