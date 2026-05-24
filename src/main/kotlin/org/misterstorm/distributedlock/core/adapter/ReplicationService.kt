package org.misterstorm.distributedlock.core.adapter

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation

interface ReplicationService {
    fun replicate(operation: LockOperation, lock: Lock): Boolean
}