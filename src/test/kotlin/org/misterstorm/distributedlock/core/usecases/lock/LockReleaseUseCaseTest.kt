package org.misterstorm.distributedlock.core.usecases.lock

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
import kotlin.test.assertTrue
import kotlin.test.fail

class LockReleaseUseCaseTest {

    @Test
    fun `should release a lock when lockOwner is the applicant for release`() = runTest{
        val lockRepository = spyk(object : TestLockRepository(){
            override fun getByKey(key: String): Lock = createLock()
            override fun release(lock: Lock): Boolean = true
        })
        val sut = LockReleaseUseCase(lockRepository)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected lock to be released successfully, but got an error: $error") },
            { lock ->
                assertAll(
                    { assertEquals(lock.key, lockCandidate.key )},
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepository.release(any()) } }
                )
            }
        )
    }

    @Test
    fun `should return error when lock does not exists`() = runTest{
        val lockRepository = spyk(object : TestLockRepository(){
            override fun getByKey(key: String): Lock? = null
        })
        val sut = LockReleaseUseCase(lockRepository)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    { assertTrue(error is BusinessError.LockNotFound) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.release(any()) } }
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
        val sut = LockReleaseUseCase(lockRepository)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            {error ->
                assertAll(
                    { assertTrue(error is BusinessError.ApplicantReleaseIsNotOwner) },
                    { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                    { verify(exactly = 0) { lockRepository.release(any()) } }
                )
            },
            { _ -> fail("Expected error, but got lock instance") }
        )
    }

}