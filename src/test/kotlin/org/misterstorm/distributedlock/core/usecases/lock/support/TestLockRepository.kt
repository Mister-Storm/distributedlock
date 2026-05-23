package org.misterstorm.distributedlock.core.usecases.lock.support

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.repository.LockRepository

open class TestLockRepository : LockRepository {
    override fun getAllLocks(): Collection<Lock> = TODO("Not yet implemented")
    override fun create(lock: Lock): Lock = TODO("Not yet implemented")
    override fun getByKey(key: String): Lock? = TODO("Not yet implemented")
    override fun release(lock: Lock): Boolean = TODO("Not yet implemented")
    override fun renew(lock: Lock): Lock = TODO("Not yet implemented")
    override fun getAllInQueue(): Collection<Lock> = TODO("Not yet implemented")
    override fun addQueue(lock: Lock): Boolean = TODO("Not yet implemented")
    override fun hasKeyInQueue(key: String): Boolean = TODO("Not yet implemented")
    override fun dequeue(key: String): Lock = TODO("Not yet implemented")
    override fun savePending(entry: ReplicaEntry) = TODO("Not yet implemented")
    override fun commit(idempotencyKey: String) = TODO("Not yet implemented")
    override fun hasPending(idempotencyKey: String): Boolean = TODO("Not yet implemented")
    override fun getPending(idempotencyKey: String): ReplicaEntry? = TODO("Not yet implemented")
    override fun loadSnapshot(locks: Collection<Lock>, queue: Collection<Lock>) = TODO("Not yet implemented")
}