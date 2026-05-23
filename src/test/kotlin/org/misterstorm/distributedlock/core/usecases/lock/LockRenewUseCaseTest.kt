package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.support.createNodeState
import org.misterstorm.distributedlock.infra.raft.RaftReplicationService
import java.time.LocalDateTime
import kotlin.test.fail

class LockRenewUseCaseTest {

    @Test
    fun `should renew lock successfully`() = runTest {
        val originalLock = createLock(expirationTime = LocalDateTime.now().minusSeconds(90))
        val lockCandidate = createLockCandidate()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String) = originalLock
            override fun renew(lock: Lock): Lock = lock
        })
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(eq(LockOperation.RENEW), any()) } returns true
        val nodeState = createNodeState()
        val sut = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected lock to be renewed successfully, but got error: $error") },
            { lock ->
                assertAll(
                    { assertEquals(lock.key, lockCandidate.key) },
                    { assertNotEquals(originalLock.expirationTime, lock.expirationTime) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.renew(any()) } }
                )
            },
        )
    }

    @Test
    fun `should return error when lock does not exists`() = runTest {
        val lockCandidate = createLockCandidate()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
        })
        val raftReplicationService = mockk<RaftReplicationService>()
        val nodeState = createNodeState()
        val sut = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.LockNotFound) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.renew(any()) } }
                )
            },
            { _ -> fail("Expected error but got Lock") },
        )
    }

    @Test
    fun `should fail the renew when lockOwner does not the applicant for release`() = runTest {
        val lockCandidate = createLockCandidate(clientId = "another_client_id")
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
        })
        val raftReplicationService = mockk<RaftReplicationService>()
        val nodeState = createNodeState()
        val sut = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.ApplicantReleaseIsNotOwner) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.renew(any()) } }
                )
            },
            { _ -> fail("Expected error but got Lock") }
        )
    }

    @Test
    fun `should fail when instance is not Leader`() = runTest {
        val lockCandidate = createLockCandidate()
        val lockRepository = spyk(object : TestLockRepository(){})
        val raftReplicationService = mockk<RaftReplicationService>()
        val nodeState = createNodeState()
        nodeState.becomeCandidate()
        val sut = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.NotLeader) },
                    { verify(exactly = 0) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.renew(any()) } }
                )
            },
            { _ -> fail("Expected error but got Lock") }
        )
    }

    @Test
    fun `should fail when quorum fail`() = runTest {
        val lockCandidate = createLockCandidate()
        val originalLock = createLock()
        val lockRepository = spyk(object : TestLockRepository(){
            override fun getByKey(key: String) = originalLock
            override fun renew(lock: Lock): Lock = lock
            override fun release(lock: Lock): Boolean = true
            override fun create(lock: Lock): Lock = lock
        })
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(eq(LockOperation.RENEW), any()) } returns false
        val nodeState = createNodeState()
        val sut = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.QuorumNotReached) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.renew(any()) } },
                    { verify(exactly = 1) { lockRepository.release(any()) } },
                    { verify(exactly = 1) { lockRepository.create(originalLock) } }
                )
            },
            { _ -> fail("Expected error but got Lock") }
        )
    }

}