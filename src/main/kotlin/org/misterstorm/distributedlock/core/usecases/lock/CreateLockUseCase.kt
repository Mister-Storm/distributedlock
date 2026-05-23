package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
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
import org.misterstorm.distributedlock.infra.raft.NodeState
import org.misterstorm.distributedlock.infra.raft.RaftReplicationService
import org.slf4j.MDC
import java.time.LocalDateTime

class CreateLockUseCase(
    private val lockRepository: LockRepository,
    private val failLockPublisher: Publisher<Lock>,
    private val expirationTime: Long,
    private val nodeState: NodeState,
    private val replicationService: RaftReplicationService,
) :
    AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(nodeState, { returnLock(input) }, { BusinessError.NotLeader(nodeState.leaderUrl.get()) })


    private fun returnLock(input: LockCandidate): Either<BusinessError, Lock> {
        MDC.clear()
        MDC.put("clientId", input.clientId)
        MDC.put("key", input.key)
        val result: Either<BusinessError, Lock> = either {
            lockRepository.getByKey(input.key)?.let { lock ->
                if (!lock.isExpired()) {
                    raise(BusinessError.LockAlreadyExists(input.key))
                }
            }
            val lock = runCatching {
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
                        LocalDateTime
                            .now().plusSeconds(expirationTime),
                    )
                )
            }.fold(
                onSuccess = { it },
                onFailure = { error ->
                    if (error is LockAlreadyExistsException) {
                        verifyQuorum( {replicationService.replicate(LockOperation.CREATE, error.existentLock) },
                            error.existentLock,
                            lockRepository::release, lockRepository::addQueue).fold(
                            { error -> raise(error) },
                            { it }
                        )
                        raise(BusinessError.LockAlreadyExists(input.key))
                    }

                    raise(BusinessError.UnexpectedException())
                }
            )
            verifyQuorum( {replicationService.replicate(LockOperation.CREATE, lock) },
                lock,
                lockRepository::release,
                lockRepository::addQueue).fold(
                { error -> raise(error) },
                { it }
            )
        }
        result.onRight { log.info("Lock created successfully") }
        result.onLeft {
            log.warn("Lock creation error")
            failLockPublisher.publish(
                Lock(
                    input.key, input.clientId,
                    LocalDateTime
                        .now().plusSeconds(expirationTime),
                )
            )
        }
        MDC.clear()
        return result
    }

}