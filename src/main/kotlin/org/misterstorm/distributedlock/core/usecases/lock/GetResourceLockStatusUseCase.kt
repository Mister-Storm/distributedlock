package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.raise.either
import org.misterstorm.distributedlock.core.adapter.LeaderStatus
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.responses.LockStatusResponse
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.support.verifyLeadership
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class GetResourceLockStatusUseCase(
    private val lockRepository: LockRepository,
    private val nodeState: LeaderStatus,
) : AbstractUseCase<String, Either<BusinessError, LockStatusResponse>>() {

    override suspend fun execute(input: String): Either<BusinessError, LockStatusResponse> {
        MDC.put("lockKey", input)
        val result = verifyLeadership(
            nodeState::isLeader,
            action = {
                either {
                    lockRepository.getByKey(input)?.let { lock ->
                        log.info("Lock status retrieved")
                        LockStatusResponse.from(lock)
                    } ?: run {
                        log.warn("Lock status query: lock not found")
                        raise(BusinessError.LockNotFound())
                    }
                }
            },
            onNotLeader = { BusinessError.NotLeader(nodeState.getLeaderUrl()) },
        )
        MDC.remove("lockKey")
        return result
    }
}
