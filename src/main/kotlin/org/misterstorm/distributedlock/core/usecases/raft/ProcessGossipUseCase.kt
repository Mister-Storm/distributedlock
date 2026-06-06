package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.GossipData
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase

class ProcessGossipUseCase(
    private val peerRepository: PeerRepository,
) : AbstractUseCase<GossipData, GossipData>() {

    override suspend fun execute(input: GossipData): GossipData {
        peerRepository.merge(input.nodes)
        peerRepository.applyRemovals(input.deadNodes)
        return GossipData(
            nodes = peerRepository.getAllNodes(),
            deadNodes = emptySet(),
        )
    }
}

