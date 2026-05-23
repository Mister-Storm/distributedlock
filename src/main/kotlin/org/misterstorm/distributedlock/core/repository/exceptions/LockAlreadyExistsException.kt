package org.misterstorm.distributedlock.core.repository.exceptions

import org.misterstorm.distributedlock.core.models.lock.Lock

class LockAlreadyExistsException(val existentLock: Lock): RuntimeException()