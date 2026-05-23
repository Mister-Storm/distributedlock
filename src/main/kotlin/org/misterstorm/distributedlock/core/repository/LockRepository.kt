package org.misterstorm.distributedlock.core.repository

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry

interface LockRepository {
    fun getAllLocks(): Collection<Lock>
    fun create(lock: Lock) : Lock
    fun getByKey(key: String): Lock?
    fun release(lock: Lock) : Boolean
    fun renew(lock: Lock) : Lock
    fun getAllInQueue(): Collection<Lock>
    fun addQueue(lock: Lock) : Boolean
    fun hasKeyInQueue(key: String) : Boolean
    fun dequeue(key: String) : Lock

    fun savePending(entry: ReplicaEntry)
    fun commit(idempotencyKey: String)
    fun hasPending(idempotencyKey: String): Boolean
    fun getPending(idempotencyKey: String): ReplicaEntry?

    fun loadSnapshot(locks: Collection<Lock>, queue: Collection<Lock>)
}