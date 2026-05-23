package org.misterstorm.distributedlock.infra.repository

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.springframework.stereotype.Repository
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

@Repository
class LockRepositoryInMemory : LockRepository {
    private val store = ConcurrentHashMap<String, Lock>()
    private val queue = CopyOnWriteArrayList<Lock>()
    private val pendingEntries = ConcurrentHashMap<String, ReplicaEntry>()
    override fun getAllLocks(): Collection<Lock> = store.values

    override fun create(lock: Lock): Lock {
        store.compute(lock.key) { _, existing ->
            if (existing != null && !existing.isExpired()) {
                throw LockAlreadyExistsException(existing)
            }
            lock
        }
        return lock
    }
    override fun getByKey(key: String): Lock? = store[key]
    override fun release(lock: Lock): Boolean = store.remove(lock.key, lock)
    override fun renew(lock: Lock): Lock {
        store.replace(lock.key, lock)
        return store.getOrDefault(lock.key, lock)
    }

    override fun getAllInQueue(): Collection<Lock> = queue

    override fun addQueue(lock: Lock): Boolean = queue.add(lock)
    override fun hasKeyInQueue(key: String): Boolean = queue.any { it.key == key }
    override fun dequeue(key: String): Lock = queue.first { it.key == key }.also { queue.remove(it) }

    override fun savePending(entry: ReplicaEntry) {
        pendingEntries[entry.idempotencyKey] = entry
    }

    override fun commit(idempotencyKey: String) {
        val entry = pendingEntries.remove(idempotencyKey) ?: return
        when (entry.operation) {
            LockOperation.CREATE -> store.compute(entry.lock.key) { _, existing ->
                if (existing != null && !existing.isExpired()) existing
                else entry.lock
            }
            LockOperation.RELEASE -> store.remove(entry.lock.key)
            LockOperation.RENEW -> store.replace(entry.lock.key, entry.lock)
        }
    }

    override fun hasPending(idempotencyKey: String): Boolean = pendingEntries.containsKey(idempotencyKey)

    override fun getPending(idempotencyKey: String): ReplicaEntry? = pendingEntries[idempotencyKey]

    override fun loadSnapshot(locks: Collection<Lock>, queue: Collection<Lock>) {
        locks.filter { !it.isExpired() }.forEach { lock ->
            store.putIfAbsent(lock.key, lock)
        }
        queue.filter { !it.isExpired() }.forEach { lock ->
            if (!hasKeyInQueue(lock.key)) {
                this.queue.add(lock)
            }
        }
    }


    fun clear() {
        store.clear()
        queue.clear()
        pendingEntries.clear()
    }
}