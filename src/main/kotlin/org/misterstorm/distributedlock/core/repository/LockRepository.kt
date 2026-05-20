package org.misterstorm.distributedlock.core.repository

import org.misterstorm.distributedlock.core.models.lock.Lock

interface LockRepository {
    fun create(lock: Lock) : Lock
    fun getByKey(key: String): Lock?
    fun release(lock: Lock) : Boolean
    fun renew(lock: Lock) : Lock
    fun addQueue(lock: Lock) : Boolean
    fun hasKeyInQueue(key: String) : Boolean
    fun dequeue(key: String) : Lock
}