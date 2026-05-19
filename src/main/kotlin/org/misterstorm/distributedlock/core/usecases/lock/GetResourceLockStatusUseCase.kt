package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.raise.either
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.responses.LockStatusResponse
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase

class GetResourceLockStatusUseCase(private val lockRepository: LockRepository) : AbstractUseCase<String, Either<BusinessError, LockStatusResponse>>() {
    override suspend fun execute(input: String): Either<BusinessError, LockStatusResponse> = either{
        lockRepository.getByKey(input)?.let { lock ->
            LockStatusResponse.from(lock)
        } ?: raise(BusinessError.LockNotFound())

    }
}