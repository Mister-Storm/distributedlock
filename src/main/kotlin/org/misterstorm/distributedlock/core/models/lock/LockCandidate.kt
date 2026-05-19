package org.misterstorm.distributedlock.core.models.lock

data class LockCandidate(
    val key: String,
    val clientId: String,
)
