package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.support.verifyLeadership
import org.misterstorm.distributedlock.core.support.verifyQuorum
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.services.RaftReplicationService
import org.slf4j.MDC

class LockRenewUseCase(
    private val lockRepository: LockRepository,
    private val nodeState: NodeState,
    private val raftReplicationService: RaftReplicationService
) : AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {

    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(
            nodeState,
            action = { renewLock(input) },
            onNotLeader = { BusinessError.NotLeader(nodeState.leaderUrl.get()) }
        )

    private fun renewLock(input: LockCandidate): Either<BusinessError, Lock> {
        MDC.put("clientId", input.clientId)
        MDC.put("lockKey", input.key)
        log.info("Attempting lock renewal")

        val result = lockRepository.getByKey(input.key)?.let { lock ->
            if (lock.lockOwner == input.clientId) {
                val renewed = lock.renew()
                lockRepository.renew(renewed)
                val quorumResult = verifyQuorum(
                    { raftReplicationService.replicate(LockOperation.RENEW, renewed) },
                    lock,
                    lockRepository::release,
                    lockRepository::create
                )
                quorumResult
                    .onRight { log.info("Lock renewed successfully") }
                    .onLeft { err ->
                        MDC.put("errorType", err::class.simpleName)
                        log.warn("Lock renewal failed: quorum not reached")
                        MDC.remove("errorType")
                    }
                if (quorumResult.isRight()) renewed.right() else quorumResult
            } else {
                MDC.put("owner", lock.lockOwner)
                log.warn("Lock renewal denied: requester is not the owner")
                MDC.remove("owner")
                BusinessError.ApplicantReleaseIsNotOwner().left()
            }
        } ?: run {
            log.warn("Lock renewal failed: lock not found")
            BusinessError.LockNotFound().left()
        }

        MDC.remove("clientId"); MDC.remove("lockKey")
        return result
    }
}