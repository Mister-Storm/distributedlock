package org.misterstorm.distributedlock.core.usecases.raft

import arrow.core.Either
import arrow.core.right
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.adapter.VoteRequester
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.raft.ElectionOutput
import org.misterstorm.distributedlock.core.models.raft.VoteInput
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class StartElectionUseCase(
    private val nodeStateRepository: NodeStateRepository,
    private val peerRepository: PeerRepository,
    private val voteRequester: VoteRequester,
) : AbstractUseCase<Unit, Either<BusinessError, ElectionOutput>>() {

    override suspend fun execute(input: Unit): Either<BusinessError, ElectionOutput> {
        synchronized(nodeStateRepository) {
            val candidateState = nodeStateRepository.getState().asCandidate()
            nodeStateRepository.saveState(candidateState)

            MDC.put("node", candidateState.name)
            MDC.put("term", candidateState.term.toString())
            log.info("Starting election")

            val voteInput = VoteInput(
                candidateName = candidateState.name,
                candidateUrl = candidateState.url,
                term = candidateState.term,
            )

            var votes = 1
            peerRepository.getPeerUrls().forEach { peerUrl ->
                val response = voteRequester.requestVote(peerUrl, voteInput)
                MDC.put("peer", peerUrl)
                if (response?.voteGranted == true) {
                    votes++
                    log.info("Vote granted by peer")
                } else {
                    log.info("Vote denied or unreachable peer")
                }
                MDC.remove("peer")
            }

            val clusterSize = peerRepository.getPeerUrls().size + 1
            val quorum = clusterSize / 2 + 1
            MDC.put("votes", votes.toString())
            MDC.put("quorum", quorum.toString())

            return if (votes >= quorum) {
                log.info("Election won")
                nodeStateRepository.saveState(nodeStateRepository.getState().asLeader())
                MDC.remove("node"); MDC.remove("term"); MDC.remove("votes"); MDC.remove("quorum")
                ElectionOutput(becameLeader = true).right()
            } else {
                log.warn("Election failed: quorum not reached")
                MDC.remove("node"); MDC.remove("term"); MDC.remove("votes"); MDC.remove("quorum")
                ElectionOutput(becameLeader = false).right()
            }
        }
    }
}

