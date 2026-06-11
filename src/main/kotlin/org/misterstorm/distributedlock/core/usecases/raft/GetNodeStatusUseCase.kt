package org.misterstorm.distributedlock.core.usecases.raft

import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.NodeStatusOutput
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.support.ClusterHealth
import org.misterstorm.distributedlock.core.usecases.AbstractUseCase

class GetNodeStatusUseCase(
    private val nodeStateRepository: NodeStateRepository,
    private val peerRepository: PeerRepository,
    private val lockRepository: LockRepository,
    private val chaosSnapshot: () -> Map<String, Any> = { emptyMap() },
) : AbstractUseCase<Unit, NodeStatusOutput>() {

    override suspend fun execute(input: Unit): NodeStatusOutput {
        val state = nodeStateRepository.getState()
        return NodeStatusOutput(
            node = state.name,
            url = state.url,
            role = state.role.name,
            term = state.term,
            leader = state.leaderName,
            leaderUrl = state.leaderUrl,
            peers = peerRepository.getRegisteredPeerUrls(),
            healthyPeers = peerRepository.getHealthyPeerUrls(),
            unhealthyPeers = peerRepository.getUnhealthyPeerUrls(),
            healthyClusterSize = ClusterHealth.healthyClusterSize(peerRepository),
            peerDetails = peerRepository.getPeerEntries(),
            knownNodes = peerRepository.getAllNodes(),
            locks = lockRepository.getAllLocks(),
            locksInQueue = lockRepository.getAllInQueue(),
            chaos = chaosSnapshot(),
        )
    }
}
