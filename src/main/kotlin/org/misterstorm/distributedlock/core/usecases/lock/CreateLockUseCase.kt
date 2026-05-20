package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.raise.either
import org.misterstorm.distributedlock.core.async.Publisher
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.repository.exceptions.LockAlreadyExistsException
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC
import java.time.Duration
import java.time.LocalDateTime

class CreateLockUseCase(
    private val lockRepository: LockRepository,
    private val failLockPublisher: Publisher<Lock>,
    private val expirationTime: Long,
) :
    AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock>{
        MDC.clear()
        MDC.put("clientId", input.clientId)
        MDC.put("key", input.key)
        val result = either {
            lockRepository.getByKey(input.key)?.let { lock ->
                if(!lock.isExpired()) {
                    raise(BusinessError.LockAlreadyExists(input.key))
                }
            }
            runCatching {
                if(lockRepository.hasKeyInQueue(input.key)){
                    lockRepository.create(lockRepository.dequeue(input.key).copy(
                        expirationTime = LocalDateTime.now().plusSeconds(expirationTime),
                    ))
                    throw LockAlreadyExistsException()
                }
                lockRepository.create(
                    Lock(
                        input.key, input.clientId, LocalDateTime
                            .now().plusSeconds(expirationTime),
                        )
                    )
            }.fold(
                onSuccess = { it },
                onFailure = { error ->
                    if (error is LockAlreadyExistsException){
                        raise(BusinessError.LockAlreadyExists(input.key))
                    }

                    raise(BusinessError.UnexpectedException()) }
            )

        }
        result.onRight { log.info("Lock created successfully") }
        result.onLeft {
            log.warn("Lock creation error")
            failLockPublisher.publish(
                Lock(
                    input.key, input.clientId, LocalDateTime
                        .now().plusSeconds(expirationTime),
                )
            )
        }
        MDC.clear()
        return result

    }

}