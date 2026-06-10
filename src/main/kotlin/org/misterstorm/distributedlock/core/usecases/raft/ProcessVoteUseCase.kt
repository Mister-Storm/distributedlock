package org.misterstorm.distributedlock.core.usecases.raft

import arrow.core.Either
import arrow.core.right
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.raft.VoteInput
import org.misterstorm.distributedlock.core.models.raft.VoteOutput
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class ProcessVoteUseCase(
    private val nodeStateRepository: NodeStateRepository,
    private val peerRepository: PeerRepository,
) : AbstractUseCase<VoteInput, Either<BusinessError, VoteOutput>>() {

    override suspend fun execute(input: VoteInput): Either<BusinessError, VoteOutput> {
        var state = nodeStateRepository.getState()
        val currentTerm = state.term

        MDC.put("candidate", input.candidateName)
        MDC.put("requestTerm", input.term.toString())
        MDC.put("currentTerm", currentTerm.toString())

        if (input.term < currentTerm) {
            log.warn("Vote denied: candidate term is stale")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            return VoteOutput(currentTerm, false).right()
        }

        if (input.term > currentTerm) {
            state = state.asFollower(input.term, state.leaderName, state.leaderUrl)
            nodeStateRepository.saveState(state)
        }

        if (state.isLeader() && input.term <= state.term) {
            log.warn("Vote denied: node is already leader")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            return VoteOutput(state.term, false).right()
        }

        val alreadyVoted = state.votedFor
        val canVote = alreadyVoted == null || alreadyVoted == input.candidateName

        return if (canVote) {
            nodeStateRepository.saveState(state.withVote(input.candidateName))
            peerRepository.merge(mapOf(input.candidateName to input.candidateUrl))
            log.info("Vote granted")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            VoteOutput(input.term, true).right()
        } else {
            log.info("Vote denied: already voted in this term")
            MDC.remove("candidate"); MDC.remove("requestTerm"); MDC.remove("currentTerm")
            VoteOutput(state.term, false).right()
        }
    }
}
