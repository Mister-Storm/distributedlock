package org.misterstorm.distributedlock.core.models.lock

data class ReplicaEntry(
    val idempotencyKey: String,
    val operation: LockOperation,
    val lock: Lock,
)
