package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.left
import org.misterstorm.distributedlock.core.adapter.LeaderStatus
import org.misterstorm.distributedlock.core.adapter.ReplicationService
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.support.verifyLeadership
import org.misterstorm.distributedlock.core.support.verifyQuorum
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC
import java.time.LocalDateTime

class LockReleaseUseCase(
    private val lockRepository: LockRepository,
    private val nodeState: LeaderStatus,
    private val replicationService: ReplicationService,
    private val expirationTime: Long,
) : AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(nodeState::isLeader, { releaseLock(input) },
            { BusinessError.NotLeader(nodeState.getLeaderUrl()) })

    private fun releaseLock(input: LockCandidate): Either<BusinessError, Lock> {
        MDC.put("clientId", input.clientId)
        MDC.put("lockKey", input.key)
        log.info("Attempting lock release")

        val result = lockRepository.getByKey(input.key)?.let { lock ->
            if (lock.lockOwner == input.clientId) {
                lockRepository.release(lock)
                val quorumResult = verifyQuorum(
                    { replicationService.replicate(LockOperation.RELEASE, lock) },
                    lock,
                    lockRepository::create
                )
                quorumResult
                    .onRight {
                        log.info("Lock released successfully")
                        promoteFromQueue(lock.key)
                    }
                    .onLeft { err ->
                        MDC.put("errorType", err::class.simpleName)
                        log.warn("Lock release failed: quorum not reached")
                        MDC.remove("errorType")
                    }
                quorumResult
            } else {
                MDC.put("owner", lock.lockOwner)
                log.warn("Lock release denied: requester is not the owner")
                MDC.remove("owner")
                BusinessError.ApplicantReleaseIsNotOwner().left()
            }
        } ?: run {
            log.warn("Lock release failed: lock not found")
            BusinessError.LockNotFound().left()
        }

        MDC.remove("clientId"); MDC.remove("lockKey")
        return result
    }

    private fun promoteFromQueue(key: String) {
        if (!lockRepository.hasKeyInQueue(key)) return
        val candidate = lockRepository.dequeue(key)
        val promoted = candidate.copy(expirationTime = LocalDateTime.now().plusSeconds(expirationTime))
        lockRepository.create(promoted)
        MDC.put("promotedOwner", promoted.lockOwner)
        MDC.put("event", "lock_promoted")
        MDC.put("operation", LockOperation.PROMOTE.name)
        log.info("Promoting queued lock after explicit release")
        val replicated = replicationService.replicate(LockOperation.PROMOTE, promoted)
        if (!replicated) {
            log.warn("Quorum not reached while promoting queued lock, re-enqueuing")
            lockRepository.release(promoted)
            lockRepository.addQueue(candidate)
            replicationService.replicate(LockOperation.ENQUEUE, candidate)
        }
        MDC.remove("promotedOwner")
        MDC.remove("event")
        MDC.remove("operation")
    }
}