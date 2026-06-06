package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.support.createNodeState
import org.misterstorm.distributedlock.infra.raft.services.RaftReplicationService
import java.time.LocalDateTime
import kotlin.test.assertTrue
import kotlin.test.fail

class LockReleaseUseCaseTest {

    private val expirationTime = 120L

    @Test
    fun `should release a lock when lockOwner is the applicant for release`() = runTest {
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected lock to be released successfully, but got an error: $error") },
            { lock ->
                assertAll(
                    { assertEquals(lock.key, lockCandidate.key) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.release(any()) } },
                    { verify(exactly = 1) { raftReplicationService.replicate(eq(LockOperation.RELEASE), any()) } },
                    { verify(exactly = 0) { lockRepository.dequeue(any()) } },
                )
            }
        )
    }

    @Test
    fun `should return error when lock does not exists`() = runTest {
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.LockNotFound) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.release(any()) } },
                    { verify(exactly = 0) { raftReplicationService.replicate(any(), any()) } },
                )
            },
            { _ -> fail("Expected an error, but got a lock.") }
        )
    }

    @Test
    fun `should fail to release a lock when lockOwner is not the applicant for release`() = runTest {
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock(lockOwner = "another_client_id")
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.ApplicantReleaseIsNotOwner) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.release(any()) } },
                    { verify(exactly = 0) { raftReplicationService.replicate(any(), any()) } },
                )
            },
            { _ -> fail("Expected error, but got lock instance") }
        )
    }

    @Test
    fun `should return error and rollback previous operations when quorum fails`() = runTest {
        val lockRepository = spyk(object : TestLockRepository() {
            override fun create(lock: Lock): Lock = lock
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns false
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.QuorumNotReached) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.release(any()) } },
                    { verify(exactly = 1) { lockRepository.create(any()) } },
                    // queue promotion must NOT happen when the RELEASE itself failed quorum
                    { verify(exactly = 0) { lockRepository.hasKeyInQueue(any()) } },
                )
            },
            { _ -> fail("Expected an error due to quorum failure, but got a lock instance.") }
        )
    }

    @Test
    fun `should promote next candidate from queue immediately after successful release`() = runTest {
        val queuedLock = createLock(lockOwner = "waiting-client")
        val promotedSlot = slot<Lock>()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
            override fun hasKeyInQueue(key: String): Boolean = true
            override fun dequeue(key: String): Lock = queuedLock
            override fun create(lock: Lock): Lock = lock
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)

        sut.execute(createLockCandidate()).fold(
            { error -> fail("Expected successful release, but got: $error") },
            { lock ->
                assertAll(
                    { assertEquals(lock.key, queuedLock.key) },
                    { verify(exactly = 1) { lockRepository.hasKeyInQueue(lock.key) } },
                    { verify(exactly = 1) { lockRepository.dequeue(lock.key) } },
                    { verify(exactly = 1) { lockRepository.create(capture(promotedSlot)) } },
                    // RELEASE for the original lock + CREATE for the promoted candidate
                    { verify(exactly = 1) { raftReplicationService.replicate(eq(LockOperation.RELEASE), any()) } },
                    { verify(exactly = 1) { raftReplicationService.replicate(eq(LockOperation.CREATE), any()) } },
                    // promoted lock keeps same owner but gets a fresh expiration
                    { assertEquals(queuedLock.lockOwner, promotedSlot.captured.lockOwner) },
                    { assertTrue(promotedSlot.captured.expirationTime > LocalDateTime.now()) },
                )
            }
        )
    }

    @Test
    fun `should re-enqueue candidate with ENQUEUE replication when promotion CREATE quorum fails`() = runTest {
        val queuedLock = createLock(lockOwner = "waiting-client")
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
            override fun hasKeyInQueue(key: String): Boolean = true
            override fun dequeue(key: String): Lock = queuedLock
            override fun create(lock: Lock): Lock = lock
            override fun addQueue(lock: Lock): Boolean = true
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        // RELEASE succeeds, CREATE for promoted candidate fails
        every { raftReplicationService.replicate(eq(LockOperation.RELEASE), any()) } returns true
        every { raftReplicationService.replicate(eq(LockOperation.CREATE), any()) } returns false
        every { raftReplicationService.replicate(eq(LockOperation.ENQUEUE), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)

        sut.execute(createLockCandidate()).fold(
            { error -> fail("Release itself should succeed, but got: $error") },
            { _ ->
                assertAll(
                    { verify(exactly = 1) { lockRepository.dequeue(any()) } },
                    // release called twice: once for original lock, once to rollback promoted lock
                    { verify(exactly = 2) { lockRepository.release(any()) } },
                    { verify(exactly = 1) { lockRepository.addQueue(any()) } },
                    { verify(exactly = 1) { raftReplicationService.replicate(eq(LockOperation.ENQUEUE), any()) } },
                )
            }
        )
    }

    @Test
    fun `should not promote from queue when queue is empty after release`() = runTest {
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService, expirationTime)

        sut.execute(createLockCandidate()).fold(
            { error -> fail("Expected successful release, but got: $error") },
            { _ ->
                assertAll(
                    { verify(exactly = 1) { lockRepository.hasKeyInQueue(any()) } },
                    { verify(exactly = 0) { lockRepository.dequeue(any()) } },
                    { verify(exactly = 1) { raftReplicationService.replicate(eq(LockOperation.RELEASE), any()) } },
                    { verify(exactly = 0) { raftReplicationService.replicate(eq(LockOperation.CREATE), any()) } },
                )
            }
        )
    }
}

