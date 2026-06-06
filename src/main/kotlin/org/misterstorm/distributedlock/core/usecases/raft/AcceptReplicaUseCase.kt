package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class AcceptReplicaUseCase(
    private val lockRepository: LockRepository,
) : AbstractUseCase<ReplicaEntry, Unit>() {

    override suspend fun execute(input: ReplicaEntry) {
        MDC.put("idempotencyKey", input.idempotencyKey)
        MDC.put("operation", input.operation.name)
        MDC.put("lockKey", input.lock.key)
        if (lockRepository.hasPending(input.idempotencyKey)) {
            log.info("Replicate request already pending (idempotent)")
            MDC.remove("idempotencyKey"); MDC.remove("operation"); MDC.remove("lockKey")
            return
        }
        lockRepository.savePending(input)
        log.info("Replicate request accepted and saved as pending")
        MDC.remove("idempotencyKey"); MDC.remove("operation"); MDC.remove("lockKey")
    }
}

