package org.misterstorm.distributedlock.core.usecases.raft

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.usecases.lock.support.TestLockRepository
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import kotlin.test.assertTrue

class AcceptAndCommitReplicaUseCaseTest {

    @Test
    fun `AcceptReplicaUseCase should save new pending entry`() = runTest {
        val entry = ReplicaEntry("key-1", LockOperation.CREATE, createLock())
        val saved = mutableListOf<ReplicaEntry>()
        val lockRepo = object : TestLockRepository() {
            override fun hasPending(idempotencyKey: String) = false
            override fun savePending(entry: ReplicaEntry) { saved.add(entry) }
        }
        val sut = AcceptReplicaUseCase(lockRepo)
        sut.execute(entry)
        assertTrue(saved.contains(entry))
    }

    @Test
    fun `AcceptReplicaUseCase should be idempotent when key already pending`() = runTest {
        val entry = ReplicaEntry("key-1", LockOperation.CREATE, createLock())
        val saved = mutableListOf<ReplicaEntry>()
        val lockRepo = object : TestLockRepository() {
            override fun hasPending(idempotencyKey: String) = true
            override fun savePending(entry: ReplicaEntry) { saved.add(entry) }
        }
        val sut = AcceptReplicaUseCase(lockRepo)
        sut.execute(entry)
        assertTrue(saved.isEmpty())
    }

    @Test
    fun `CommitReplicaUseCase should call commit on repository`() = runTest {
        val committed = mutableListOf<String>()
        val lockRepo = object : TestLockRepository() {
            override fun commit(idempotencyKey: String) { committed.add(idempotencyKey) }
        }
        val sut = CommitReplicaUseCase(lockRepo)
        sut.execute("idempotency-key")
        assertTrue(committed.contains("idempotency-key"))
    }

    @Test
    fun `CommitReplicaUseCase with ENQUEUE operation should add lock to queue`() = runTest {
        val queuedLocks = mutableListOf<Lock>()
        val lockToEnqueue = createLock()
        val entry = ReplicaEntry("enqueue-key-1", LockOperation.ENQUEUE, lockToEnqueue)
        val lockRepo = object : TestLockRepository() {
            private val pending = mutableMapOf<String, ReplicaEntry>()
            override fun hasPending(idempotencyKey: String) = pending.containsKey(idempotencyKey)
            override fun savePending(entry: ReplicaEntry) { pending[entry.idempotencyKey] = entry }
            override fun commit(idempotencyKey: String) {
                val e = pending.remove(idempotencyKey) ?: return
                if (e.operation == LockOperation.ENQUEUE) queuedLocks.add(e.lock)
            }
        }
        val acceptSut = AcceptReplicaUseCase(lockRepo)
        val commitSut = CommitReplicaUseCase(lockRepo)
        acceptSut.execute(entry)
        commitSut.execute("enqueue-key-1")
        assertTrue(queuedLocks.contains(lockToEnqueue), "Lock should have been enqueued after commit")
    }
}

