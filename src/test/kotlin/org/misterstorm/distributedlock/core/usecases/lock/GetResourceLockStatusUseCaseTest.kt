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
import org.misterstorm.distributedlock.core.models.responses.LockStatus
import org.misterstorm.distributedlock.core.models.responses.LockStatusResponse
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import java.time.LocalDateTime
import kotlin.test.fail

class GetResourceLockStatusUseCaseTest {

    @Test
    fun `Should return LockStatusResponse Locked when resource is locked`() {
        val key = "any-key"
        runTest{
            val lockRepository = spyk(object : TestLockRepository(){
                override fun getByKey(key: String): Lock = createLock()
            })
            val sut = GetResourceLockStatusUseCase(lockRepository)
            sut.execute(key).fold(
                {error -> fail("Expected to return LockStatusResponse Locked but got error: $error")},
                {result ->
                    assertAll(
                        { assertTrue(result is LockStatusResponse.Locked) },
                        { assertEquals((result as LockStatusResponse.Locked).status, LockStatus.LOCKED ) },
                        { assertTrue((result as LockStatusResponse.Locked).lockedUntil.isAfter(LocalDateTime.now())) },
                        { verify(exactly = 1) { lockRepository.getByKey(key) } }
                    )
                }
            )
        }
    }

    @Test
    fun `Should return LockStatusResponse NotLocked when resource is locked and lock is expired`() {
        val key = "any-key"
        runTest {
            val lockRepository = spyk(object : TestLockRepository() {
                override fun getByKey(key: String): Lock =
                    createLock(expirationTime = LocalDateTime.now().minusSeconds(12))
            })
            val sut = GetResourceLockStatusUseCase(lockRepository)
            sut.execute(key).fold(
                { error -> fail("Expected to return LockStatusResponse Locked but got error: $error") },
                { result ->
                    assertAll(
                        { assertTrue(result is LockStatusResponse.NotLocked) },
                        { assertEquals((result as LockStatusResponse.NotLocked).status, LockStatus.AVAILABLE) },
                        { verify(exactly = 1) { lockRepository.getByKey(key) } }
                    )
                }
            )
        }
    }
        @Test
        fun `Should return LockNotFound when resource is not found`() =
            runTest{
                val lockRepository = spyk(object : TestLockRepository(){
                    override fun getByKey(key: String): Lock? = null
                })
                val sut = GetResourceLockStatusUseCase(lockRepository)
                sut.execute("key").fold(
                    {error -> assertTrue(error is BusinessError.LockNotFound) },
                    {_ -> fail("Expected to return LockNotFound but got LockStatusResponse") }
                )
            }

}