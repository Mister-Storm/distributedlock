package org.misterstorm.distributedlock.core.usecases.raft

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.raft.HeartbeatInput
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class ProcessHeartbeatUseCase(
    private val nodeStateRepository: NodeStateRepository,
    private val lockRepository: LockRepository,
) : AbstractUseCase<HeartbeatInput, Either<BusinessError, Unit>>() {

    override suspend fun execute(input: HeartbeatInput): Either<BusinessError, Unit> {
        val state = nodeStateRepository.getState()
        MDC.put("leaderName", input.leaderName)
        MDC.put("term", input.term.toString())

        if (input.term < state.term || state.isLeader()) {
            log.warn("Heartbeat rejected: stale term or node is already leader")
            MDC.remove("leaderName"); MDC.remove("term")
            return if (state.isLeader()) BusinessError.AlreadyLeader().left()
            else BusinessError.StaleTerm().left()
        }

        nodeStateRepository.saveState(
            state.asFollower(input.term, input.leaderName, input.leaderUrl)
        )

        input.recentCommits.forEach { key ->
            if (lockRepository.hasPending(key)) {
                lockRepository.commit(key)
            }
        }

        log.info("Heartbeat accepted")
        MDC.remove("leaderName"); MDC.remove("term")
        return Unit.right()
    }
}

