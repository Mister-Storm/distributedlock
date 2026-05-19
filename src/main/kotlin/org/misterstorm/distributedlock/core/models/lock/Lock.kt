package org.misterstorm.distributedlock.core.models.lock

import java.time.LocalDateTime

data class Lock(
    val key: String,
    val lockOwner: String,
    val expirationTime: LocalDateTime,
) {
    fun isExpired() = expirationTime.isBefore(LocalDateTime.now())
}