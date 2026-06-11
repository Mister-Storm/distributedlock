package org.misterstorm.distributedlock.core.models.raft

data class VoteInput(
    val candidateName: String,
    val candidateUrl: String,
    val term: Long,
)

data class VoteOutput(
    val term: Long,
    val voteGranted: Boolean,
)

data class HeartbeatInput(
    val leaderName: String,
    val leaderUrl: String,
    val term: Long,
    val recentCommits: List<String> = emptyList(),
)

data class GossipData(
    val nodes: Map<String, String>,
)

data class JoinInput(
    val name: String,
    val url: String,
)

data class ExcludeVoteInput(val suspectUrl: String)
data class ExcludeVoteOutput(val exclude: Boolean)

data class NodeStatusOutput(
    val node: String,
    val url: String,
    val role: String,
    val term: Long,
    val leader: String?,
    val leaderUrl: String?,
    val peers: List<String>,
    val healthyPeers: List<String>,
    val unhealthyPeers: List<String>,
    val healthyClusterSize: Int,
    val peerDetails: List<PeerEntry>,
    val knownNodes: Map<String, String>,
    val locks: Collection<Any>,
    val locksInQueue: Collection<Any>,
    val chaos: Map<String, Any> = emptyMap(),
)

data class SnapshotData(
    val locks: Collection<Any>,
    val queue: Collection<Any>,
)

data class ElectionOutput(val becameLeader: Boolean)

