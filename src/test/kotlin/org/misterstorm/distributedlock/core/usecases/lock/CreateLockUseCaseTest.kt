package org.misterstorm.distributedlock.core.usecases.lock

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.async.Publisher
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.core.usecases.lock.support.createLockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.support.createNodeState
import org.misterstorm.distributedlock.infra.raft.NodeState
import org.misterstorm.distributedlock.infra.raft.RaftReplicationService
import org.misterstorm.distributedlock.web.routes.lock
import java.time.LocalDateTime
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class CreateLockUseCaseTest {

    private val expirationTime = 120L

    @Test
    fun `should create a new lock`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = lock
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        every { raftReplicationServiceMock.replicate(any(), any()) } returns true
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
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
                    verify(exactly = 0) { failLockPublisher.publish(lock) }
                    verify(exactly = 1) { raftReplicationServiceMock.replicate(eq(LockOperation.CREATE), any()) }
                }
            )
    }

    @Test
    fun `should fail to create a new lock when lock still exists and is valid`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock()
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    {
                        assertTrue(
                            error is BusinessError.LockAlreadyExists,
                            "Expected a BusinessError, but got: $error"
                        )
                    },
                    { verify(exactly = 1) { failLockPublisher.publish(any()) } },
                )
            },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should create a lock when lock still exists and is expired`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock(expirationTime = LocalDateTime.now().minusSeconds(1))
            override fun create(lock: Lock): Lock = lock
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        every { raftReplicationServiceMock.replicate(any(), any()) } returns true
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate(clientId = "anotherClientId")
        sut.execute(lockCandidate).fold(
            { error -> fail("Expected a lock to be created, but got an error: $error") },
            { lock ->
                assertEquals(lockCandidate.key, lock.key)
                assertEquals(lockCandidate.clientId, lock.lockOwner)
                assertTrue(lock.expirationTime > LocalDateTime.now(), "Expiration time should be in the future")
                verify(exactly = 1) { lockRepositoryStub.getByKey(lockCandidate.key) }
                verify(exactly = 1) { lockRepositoryStub.create(any()) }
                verify(exactly = 1) { raftReplicationServiceMock.replicate(eq(LockOperation.CREATE), any()) }
            }
        )
    }

    @Test
    fun `should fail to create a new lock when LockAlreadyExistsException was throw`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = throw LockAlreadyExistsException(lock)
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        every { raftReplicationServiceMock.replicate(any(), any()) } returns true
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    {
                        assertTrue(
                            error is BusinessError.LockAlreadyExists,
                            "Expected a BusinessError, but got: $error"
                        )
                    },
                    { verify(exactly = 1) { failLockPublisher.publish(any()) } },
                )
            },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should fail to create a new lock when an Unexpected exception was throw`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = throw IllegalAccessError()
            override fun hasKeyInQueue(key: String): Boolean = false
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    {
                        assertTrue(
                            error is BusinessError.UnexpectedException,
                            "Expected a BusinessError, but got: $error"
                        )
                    },
                    { verify(exactly = 1) { failLockPublisher.publish(any()) } },
                )
            },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should prioritize queue locks when there is at least one lock waiting in the queue`() = runTest {
        val lockInQueue = createLock(lockOwner = "queuedClientId")
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock = createLock(expirationTime = LocalDateTime.now().minusSeconds(1))
            override fun hasKeyInQueue(key: String): Boolean = true
            override fun dequeue(key: String): Lock = lockInQueue
            override fun create(lock: Lock): Lock = lock
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        every { raftReplicationServiceMock.replicate(any(), any()) } returns true
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate()
        val captor = slot<Lock>()
        sut.execute(lockCandidate).fold(
            { error ->
                assertAll(
                    {
                        assertTrue(
                            error is BusinessError.LockAlreadyExists,
                            "Expected a BusinessError, but got: $error"
                        )
                    },
                    { verify(exactly = 1) { lockRepositoryStub.getByKey(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepositoryStub.hasKeyInQueue(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepositoryStub.dequeue(lockCandidate.key) } },
                    { verify(exactly = 1) { lockRepositoryStub.create(capture(captor)) } },
                    { verify(exactly = 1) { failLockPublisher.publish(any()) } },
                    { assertEquals(lockInQueue.key, captor.captured.key) },
                    { assertEquals(lockInQueue.lockOwner, captor.captured.lockOwner) },
                    { assertNotEquals(lockInQueue.expirationTime, captor.captured.expirationTime) },
                    {
                        verify(exactly = 1) { raftReplicationServiceMock.replicate(eq(LockOperation.CREATE), any()) }
                    }
                )
            },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should return without process when node is not leader`() = runTest {
        val nodeState = NodeState(
            nodeName = "node1",
            nodeUrl = "http://localhost:8080",
            electionTimeout = 1000L
        )
        val lockRepositoryStub = spyk(object : TestLockRepository() {})
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = TODO("Not yet implemented")
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, nodeState, raftReplicationServiceMock
        )
        sut.execute(createLockCandidate()).fold(
            { error -> assertTrue(error is BusinessError.NotLeader, "Expected a NotLeader error, but got: $error") },
            { _ -> fail("Expected an error to be returned, but got a lock") }
        )
    }

    @Test
    fun `should fail and return QuorumNotReached when quorum fail`() = runTest {
        val lockRepositoryStub = spyk(object : TestLockRepository() {
            override fun getByKey(key: String): Lock? = null
            override fun create(lock: Lock): Lock = lock
            override fun hasKeyInQueue(key: String): Boolean = false
            override fun release(lock: Lock): Boolean = true
            override fun addQueue(lock: Lock): Boolean = true
        })
        val failLockPublisher = spyk(object : Publisher<Lock> {
            override fun publish(value: Lock) = Unit
        })
        val raftReplicationServiceMock = mockk<RaftReplicationService>()
        every { raftReplicationServiceMock.replicate(any(), any()) } returns false
        val sut = CreateLockUseCase(
            lockRepositoryStub, failLockPublisher,
            expirationTime, createNodeState(), raftReplicationServiceMock
        )
        val lockCandidate = createLockCandidate()

        sut.execute(lockCandidate)
            .fold(
                { error ->
                    assertAll(
                        {
                            assertTrue(
                                error is BusinessError.QuorumNotReached,
                                "Expected a BusinessError, but got: $error"
                            )
                        },
                        { verify(exactly = 1) { lockRepositoryStub.release(any()) } },
                        { verify(exactly = 1) { lockRepositoryStub.addQueue(any()) } },
                    )
                },
                { _ -> fail("Expected an error to be returned, but got a lock") }
            )
    }

}