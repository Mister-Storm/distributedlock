package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class CommitReplicaUseCase(
    private val lockRepository: LockRepository,
) : AbstractUseCase<String, Unit>() {

    override suspend fun execute(input: String) {
        MDC.put("idempotencyKey", input)
        lockRepository.commit(input)
        log.info("Commit applied")
        MDC.remove("idempotencyKey")
    }
}

