package org.misterstorm.distributedlock.core.usecases.lock

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.Raise
import arrow.core.raise.either
import arrow.core.right
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

class LockRenewUseCase(
    private val lockRepository: LockRepository,
    private val nodeState: NodeState,
    private val raftReplicationService: RaftReplicationService
): AbstractUseCase<LockCandidate, Either<BusinessError, Lock>>() {
    override suspend fun execute(input: LockCandidate): Either<BusinessError, Lock> =
        verifyLeadership(
            nodeState,
            action = { renewLock(input) },
            onNotLeader = { BusinessError.NotLeader(nodeState.leaderUrl.get()) })


    private fun renewLock(input: LockCandidate): Either<BusinessError, Lock> =
        lockRepository.getByKey(input.key)?.let { lock ->
            if (lock.lockOwner == input.clientId) {
                val renewLock = lock.renew()
                lockRepository.renew(renewLock)
                verifyQuorum( { raftReplicationService.replicate(LockOperation.RENEW, renewLock) },
                    lock,
                    lockRepository::release,
                    lockRepository::create).onRight { return renewLock.right() }
            } else {
                BusinessError.ApplicantReleaseIsNotOwner().left()
            }
        } ?: BusinessError.LockNotFound().left()
}