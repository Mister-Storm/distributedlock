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

        val result: Either<BusinessError, Lock> = either {
            verifyExistentLock(input)
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

        result.onRight {
            log.info("Lock created successfully")
        }
        result.onLeft { error ->
            MDC.put("errorType", error::class.simpleName)
            log.warn("Lock creation failed")
            MDC.remove("errorType")
            failLockPublisher.publish(
                Lock(input.key, input.clientId, LocalDateTime.now().plusSeconds(expirationTime))
            )
        }

        MDC.remove("clientId")
        MDC.remove("lockKey")
        return result
    }

    private fun Raise<BusinessError>.getLock(input: LockCandidate): Lock = runCatching {
        if (lockRepository.hasKeyInQueue(input.key)) {
            val existentLock = lockRepository.dequeue(input.key).copy(
                expirationTime = LocalDateTime.now().plusSeconds(expirationTime)
            )
            lockRepository.create(existentLock)
            throw LockAlreadyExistsException(existentLock)
        }
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
                verifyQuorum(
                    { replicationService.replicate(LockOperation.CREATE, error.existentLock) },
                    error.existentLock,
                    lockRepository::release, lockRepository::addQueue
                ).fold(
                    { err -> raise(err) },
                    { it }
                )
                raise(BusinessError.LockAlreadyExists(input.key))
            }
            log.error("Unexpected error during lock creation")
            raise(BusinessError.UnexpectedException())
        }
    )

    private fun Raise<BusinessError>.verifyExistentLock(input: LockCandidate) {
        lockRepository.getByKey(input.key)?.let { lock ->
            if (!lock.isExpired()) {
                MDC.put("existingOwner", lock.lockOwner)
                log.warn("Lock creation denied: lock already exists and is not expired")
                MDC.remove("existingOwner")
                raise(BusinessError.LockAlreadyExists(input.key))
            }
        }
    }

}