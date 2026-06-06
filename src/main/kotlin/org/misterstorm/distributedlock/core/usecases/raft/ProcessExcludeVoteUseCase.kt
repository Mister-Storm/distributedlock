package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.adapter.NodeReachabilityChecker
import org.misterstorm.distributedlock.core.models.raft.ExcludeVoteInput
import org.misterstorm.distributedlock.core.models.raft.ExcludeVoteOutput
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class ProcessExcludeVoteUseCase(
    private val reachabilityChecker: NodeReachabilityChecker,
) : AbstractUseCase<ExcludeVoteInput, ExcludeVoteOutput>() {

    override suspend fun execute(input: ExcludeVoteInput): ExcludeVoteOutput {
        MDC.put("suspectUrl", input.suspectUrl)
        val canReach = reachabilityChecker.isReachable(input.suspectUrl)
        MDC.put("canReach", canReach.toString())
        log.info("Exclude vote cast")
        MDC.remove("suspectUrl"); MDC.remove("canReach")
        return ExcludeVoteOutput(exclude = !canReach)
    }
}

