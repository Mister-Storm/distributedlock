package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.GossipData
import org.misterstorm.distributedlock.core.models.raft.JoinInput
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase
import org.slf4j.MDC

class ProcessJoinUseCase(
    private val peerRepository: PeerRepository,
) : AbstractUseCase<JoinInput, GossipData>() {

    override suspend fun execute(input: JoinInput): GossipData {
        MDC.put("joiningNode", input.name)
        MDC.put("joiningUrl", input.url)
        peerRepository.merge(mapOf(input.name to input.url))
        log.info("Node joined the cluster")
        MDC.remove("joiningNode"); MDC.remove("joiningUrl")
        return GossipData(nodes = peerRepository.getAllNodes())
    }
}

