package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.support.createNodeState
import org.misterstorm.distributedlock.infra.raft.services.RaftReplicationService
import kotlin.test.assertTrue
import kotlin.test.fail

class LockReleaseUseCaseTest {

    @Test
    fun `should release a lock when lockOwner is the applicant for release`() = runTest{
        val lockRepository = spyk(object : TestLockRepository(){
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected lock to be released successfully, but got an error: $error") },
            { lock ->
                assertAll(
                    { assertEquals(lock.key, lockCandidate.key )},
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.release(any()) } },
                    { verify(exactly = 1) { raftReplicationService.replicate(any(), any()) } },
                )
            }
        )
    }

    @Test
    fun `should return error when lock does not exists`() = runTest{
        val lockRepository = spyk(object : TestLockRepository(){
            override fun getByKey(key: String): Lock? = null
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns true
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService)
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
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            {error ->
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
        val lockRepository = spyk(object : TestLockRepository(){
            override fun create(lock: Lock): Lock = lock
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
        })
        val nodeState = createNodeState()
        val raftReplicationService = mockk<RaftReplicationService>()
        every { raftReplicationService.replicate(any(), any()) } returns false
        val sut = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> assertAll(
                { assertTrue(error is BusinessError.QuorumNotReached) },
                { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                { verify(exactly = 1) { lockRepository.release(any()) } },
                { verify(exactly = 1) { lockRepository.create(any()) } },
            )},
            { _ -> fail("Expected an error due to quorum failure, but got a lock instance.") }
        )
    }

}