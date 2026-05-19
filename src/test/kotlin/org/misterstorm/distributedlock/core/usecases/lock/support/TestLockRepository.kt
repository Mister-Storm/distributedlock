package org.misterstorm.distributedlock.core.usecases.lock.support

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.repository.LockRepository

open class TestLockRepository : LockRepository {
    override fun create(lock: Lock): Lock = TODO("Not yet implemented")
    override fun getByKey(key: String): Lock? = TODO("Not yet implemented")
    override fun release(lock: Lock): Boolean = TODO("Not yet implemented")
    override fun renew(lock: Lock): Lock = TODO("Not yet implemented")
    override fun addQueue(lock: Lock): Boolean = TODO("Not yet implemented")

}