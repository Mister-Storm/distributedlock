package org.misterstorm.distributedlock.core.usecases.raft

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.raft.SnapshotData
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class GetSnapshotUseCase(
    private val nodeStateRepository: NodeStateRepository,
    private val lockRepository: LockRepository,
) : AbstractUseCase<Unit, Either<BusinessError, SnapshotData>>() {

    override suspend fun execute(input: Unit): Either<BusinessError, SnapshotData> {
        val state = nodeStateRepository.getState()
        MDC.put("node", state.name)
        if (!state.isLeader()) {
            log.warn("Snapshot request rejected: node is not the leader")
            MDC.remove("node")
            return BusinessError.NotLeader(state.leaderUrl).left()
        }
        log.info("Snapshot requested by follower")
        MDC.remove("node")
        return SnapshotData(
            locks = lockRepository.getAllLocks().filter { !it.isExpired() },
            queue = lockRepository.getAllInQueue(),
        ).right()
    }
}

