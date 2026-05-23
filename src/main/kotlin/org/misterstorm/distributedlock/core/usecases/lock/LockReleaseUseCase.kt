package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.left
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.support.verifyLeadership
import org.misterstorm.distributedlock.core.support.verifyQuorum
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.misterstorm.distributedlock.infra.raft.NodeState
import org.misterstorm.distributedlock.infra.raft.RaftReplicationService

class LockReleaseUseCase(
    private val lockRepository: LockRepository,
    private val nodeState: NodeState,
    private val raftReplicationService: RaftReplicationService
) : AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(nodeState, { releaseLock(input) },
            { BusinessError.NotLeader(nodeState.leaderUrl.get()) })

    private fun releaseLock(input: LockCandidate): Either<BusinessError, Lock> = lockRepository.getByKey(input.key)?.let {
        if (it.lockOwner == input.clientId) {
            lockRepository.release(it)
            verifyQuorum({ raftReplicationService.replicate(LockOperation.RELEASE, it) },
                it,
                lockRepository::create)
        } else {
            BusinessError.ApplicantReleaseIsNotOwner().left()
        }

    } ?: BusinessError.LockNotFound().left()
}