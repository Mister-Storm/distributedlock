package org.misterstorm.distributedlock.core.models.responses

import org.misterstorm.distributedlock.core.models.lock.Lock
import java.time.LocalDateTime

interface LockStatusResponse{
    data class Locked(
        val key: String,
        val lockedUntil: LocalDateTime,
        val lockedFor: String,
        val status: LockStatus = LockStatus.LOCKED,
    ) : LockStatusResponse
    data class NotLocked(
        val key: String,
        val status: LockStatus = LockStatus.AVAILABLE,
    ): LockStatusResponse

    companion object{
        fun from(lock : Lock) : LockStatusResponse {
            return if(lock.isExpired()) {
                NotLocked(
                    key = lock.key,
                )
            } else {
                Locked(
                    key = lock.key,
                    lockedUntil = lock.expirationTime,
                    lockedFor = lock.lockOwner,
                )
            }
        }
    }
}

enum class LockStatus {
    LOCKED,
    AVAILABLE,
}