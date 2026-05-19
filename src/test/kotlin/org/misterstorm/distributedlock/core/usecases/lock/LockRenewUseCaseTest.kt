package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import kotlin.test.fail

class LockRenewUseCaseTest {

    @Test
    fun `should renew lock successfully`() {
        val lockCandidate = createLockCandidate()
        runTest {
            val lockRepository = spyk(object : TestLockRepository() {
                override fun getByKey(key: String) = createLock()
                override fun renew(lock: Lock): Lock = lock
            })
            val sut = LockRenewUseCase(lockRepository)
            sut.execute(lockCandidate).fold(
                { error -> fail("Expected lock to be renewed successfully, but got error: $error") },
                { lock ->
                    assertAll(
                        { assertEquals(lock.key, lockCandidate.key) },
                        { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key)} },
                        { verify(exactly = 1) { lockRepository.renew(any()) } }
                    )},
            )
        }
    }

    @Test
    fun `should return error when lock does not exists`() = runTest {
        val lockCandidate = createLockCandidate()
        val lockRepository = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
        })
        val sut = LockRenewUseCase(lockRepository)
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
        val sut = LockRenewUseCase(lockRepository)
        sut.execute(lockCandidate).fold(
            { error -> assertAll(
                { assertTrue(error is BusinessError.ApplicantReleaseIsNotOwner) },
                { verify(exactly = 1) { lockRepository.getByKey(lockCandidate.key) } },
                { verify(exactly = 0) { lockRepository.renew(any()) } }
            )},
            { _ -> fail("Expected error but got Lock") }
        )
    }

}