package org.misterstorm.distributedlock.infra.repository

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.springframework.stereotype.Repository

@Repository
class LockRepositoryInMemory : LockRepository {
    private val store = mutableMapOf<String, Lock>()
    private val queue = mutableListOf<Lock>()
    override fun create(lock: Lock): Lock {
        store.compute(lock.key) { _, existing ->
            if (existing != null && !existing.isExpired()) {
                throw LockAlreadyExistsException()
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

    override fun addQueue(lock: Lock): Boolean = queue.add(lock)

    fun clear() {
        store.clear()
    }
}