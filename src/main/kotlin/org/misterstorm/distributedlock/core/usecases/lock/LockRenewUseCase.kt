package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.raise.either
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase

class LockRenewUseCase(private val lockRepository: LockRepository): AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> = either{
        lockRepository.getByKey(input.key)?.let {
            lock ->
            if(lock.lockOwner == input.clientId) {
                lockRepository.renew(lock)
            } else {
                raise(BusinessError.ApplicantReleaseIsNotOwner())
            }
        } ?:
        raise(BusinessError.LockNotFound())
    }
}