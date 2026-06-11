package org.misterstorm.distributedlock.core.support

import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.raft.PeerEntry
import org.misterstorm.distributedlock.core.models.raft.PeerReachability

object ClusterHealth {

    fun healthyClusterSize(peerRepository: PeerRepository): Int =
        peerRepository.getHealthyPeerUrls().size + 1

    fun registeredClusterSize(peerRepository: PeerRepository): Int =
        peerRepository.getRegisteredPeerUrls().size + 1

    fun quorum(clusterSize: Int): Int = clusterSize / 2 + 1

    fun healthyQuorum(peerRepository: PeerRepository): Int =
        quorum(healthyClusterSize(peerRepository))

    fun healthyPeers(peerRepository: PeerRepository): List<PeerEntry> =
        peerRepository.getPeerEntries().filter { it.reachability == PeerReachability.HEALTHY }

    fun unhealthyPeers(peerRepository: PeerRepository): List<PeerEntry> =
        peerRepository.getPeerEntries().filter { it.reachability == PeerReachability.UNHEALTHY }
}
