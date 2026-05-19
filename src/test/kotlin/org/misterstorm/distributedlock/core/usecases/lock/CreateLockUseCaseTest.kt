package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class CreateLockUseCaseTest {

    private val expirationTime = 120L
    @Test
    fun `should create a new lock`() = runTest{
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = lock
        })
        val sut = CreateLockUseCase(lockRepositoryStub, expirationTime)
        val lockCandidate = createLockCandidate()

        sut.execute(lockCandidate)
            .fold(
                { error -> fail("Expected a lock to be created, but got an error: $error") },
                { lock ->
                    assertEquals(lockCandidate.key, lock.key)
                    assertEquals(lockCandidate.clientId, lock.lockOwner)
                    assertTrue(lock.expirationTime > LocalDateTime.now(), "Expiration time should be in the future")
                    verify(exactly = 1) { lockRepositoryStub.getByKey(lockCandidate.key) }
                    verify(exactly = 1) { lockRepositoryStub.create(any()) }
                }
            )
    }

    @Test
    fun `should fail to create a new lock when lock still exists and is valid`() = runTest{
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
        })
        val sut = CreateLockUseCase(lockRepositoryStub, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> assertTrue(error is BusinessError.LockAlreadyExists, "Expected a BusinessError, but got: $error") },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should create a lock when lock still exists and is expired`() = runTest{
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock(expirationTime = LocalDateTime.now().minusSeconds(1))
            override fun create(lock: Lock): Lock = lock
        })
        val sut = CreateLockUseCase(lockRepositoryStub, expirationTime)
        val lockCandidate = createLockCandidate(clientId = "anotherClientId")
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected a lock to be created, but got an error: $error") },
            { lock ->
                assertEquals(lockCandidate.key, lock.key)
                assertEquals(lockCandidate.clientId, lock.lockOwner)
                assertTrue(lock.expirationTime > LocalDateTime.now(), "Expiration time should be in the future")
                verify(exactly = 1) { lockRepositoryStub.getByKey(lockCandidate.key) }
                verify(exactly = 1) { lockRepositoryStub.create(any()) }
            }
        )
    }

    @Test
    fun `should fail to create a new lock when LockAlreadyExistsException was throw`() = runTest{
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = throw LockAlreadyExistsException()
        })
        val sut = CreateLockUseCase(lockRepositoryStub, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> assertTrue(error is BusinessError.LockAlreadyExists, "Expected a BusinessError, but got: $error") },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should fail to create a new lock when an Unexpected exception was throw`() = runTest{
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = throw IllegalAccessError()
        })
        val sut = CreateLockUseCase(lockRepositoryStub, expirationTime)
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error -> assertTrue(error is BusinessError.UnexpectedException, "Expected a BusinessError, but got: $error") },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

}