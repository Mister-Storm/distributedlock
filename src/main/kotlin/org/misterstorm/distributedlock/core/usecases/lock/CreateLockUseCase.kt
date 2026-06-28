package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.raise.Raise
import arrow.core.raise.either
import org.misterstorm.distributedlock.core.adapter.LeaderStatus
import org.misterstorm.distributedlock.core.adapter.ReplicationService
import org.misterstorm.distributedlock.core.async.Publisher
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.misterstorm.distributedlock.core.support.verifyLeadership
import org.misterstorm.distributedlock.core.support.verifyQuorum
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC
import java.time.LocalDateTime

class CreateLockUseCase(
    private val lockRepository: LockRepository,
    private val failLockPublisher: Publisher<Lock>,
    private val expirationTime: Long,
    private val nodeState: LeaderStatus,
    private val replicationService: ReplicationService,
) : AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {

    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(nodeState::isLeader, { returnLock(input) },
            { BusinessError.NotLeader(nodeState.getLeaderUrl()) })

    private fun returnLock(input: LockCandidate): Either<BusinessError, Lock> {
        MDC.put("clientId", input.clientId)
        MDC.put("lockKey", input.key)

        val existing = lockRepository.getByKey(input.key)
        existing?.let { lock ->
            if (!lock.isExpired()) {
                if (lock.lockOwner == input.clientId) {
                    log.info("Lock already held by requester — idempotent success")
                    clearMdc()
                    return Either.Right(lock)
                }
                if (lockRepository.hasClientInQueue(input.key, input.clientId)) {
                    log.info("Client already in queue for key — idempotent response")
                    clearMdc()
                    return Either.Left(BusinessError.AlreadyInQueue(input.key))
                }
            }
        }

        val result: Either<BusinessError, Lock> = either {
            if (existing != null && !existing.isExpired()) {
                MDC.put("existingOwner", existing.lockOwner)
                log.warn("Lock creation denied: lock already exists and is not expired")
                MDC.remove("existingOwner")
                raise(BusinessError.LockAlreadyExists(input.key))
            }
            val lock = getLock(input)
            verifyQuorum(
                { replicationService.replicate(LockOperation.CREATE, lock) },
                lock,
                lockRepository::release,
                lockRepository::addQueue
            ).fold(
                { err -> raise(err) },
                { it }
            )
        }

        val queuedLock = Lock(input.key, input.clientId, LocalDateTime.now().plusSeconds(expirationTime))

        result.onRight {
            log.info("Lock created successfully")
        }
        result.onLeft { error ->
            MDC.put("errorType", error::class.simpleName)
            log.warn("Lock creation failed")
            MDC.remove("errorType")
            if (error is BusinessError.LockAlreadyExists) {
                val enqueued = replicationService.replicate(LockOperation.ENQUEUE, queuedLock)
                if (!enqueued) {
                    log.warn("ENQUEUE replication failed: queue entry will only exist on this node until next snapshot sync")
                }
                failLockPublisher.publish(queuedLock)
            }
        }

        clearMdc()
        return result
    }

    private fun clearMdc() {
        MDC.remove("clientId")
        MDC.remove("lockKey")
    }

    private fun Raise<BusinessError>.getLock(input: LockCandidate): Lock {
        if (lockRepository.hasKeyInQueue(input.key)) {
            val candidate = lockRepository.dequeue(input.key)
            val promoted = candidate.copy(
                expirationTime = LocalDateTime.now().plusSeconds(expirationTime),
            )
            lockRepository.create(promoted)
            verifyQuorum(
                { replicationService.replicate(LockOperation.PROMOTE, promoted) },
                promoted,
                lockRepository::release,
                { lockRepository.addQueue(candidate) },
            ).fold(
                { err -> raise(err) },
                { },
            )
            if (promoted.lockOwner != input.clientId) {
                raise(BusinessError.LockAlreadyExists(input.key))
            }
            return promoted
        }

        return runCatching {
            lockRepository.create(
                Lock(
                    input.key, input.clientId,
                    LocalDateTime.now().plusSeconds(expirationTime),
                )
            )
        }.fold(
            onSuccess = { it },
            onFailure = { error ->
                if (error is LockAlreadyExistsException) {
                    raise(BusinessError.LockAlreadyExists(input.key))
                }
                log.error("Unexpected error during lock creation")
                raise(BusinessError.UnexpectedException())
            }
        )
    }

}
