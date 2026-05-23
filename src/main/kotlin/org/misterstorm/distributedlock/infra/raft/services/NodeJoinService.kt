package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

@Component
class NodeJoinService(
    private val nodeRegistry: NodeRegistry,
    private val nodeState: NodeState,
    private val electionService: ElectionService,
    private val httpClient: HttpClient,
    private val objectMapper: ObjectMapper,
    private val lockRepository: LockRepository,
) : ApplicationRunner {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run(args: ApplicationArguments) {
        val seeds = nodeRegistry.getPeerUrls()
        if (seeds.isEmpty()) {
            logger.info("[${nodeState.nodeName}] No seeds configured, starting election immediately")
            electionService.startElection()
            return
        }

        var foundAnyPeer = false

        seeds.forEach { seed ->
            runCatching {
                val joinRequest = JoinRequest(name = nodeState.nodeName, url = nodeState.nodeUrl)
                val body = objectMapper.writeValueAsString(joinRequest)
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$seed/raft/join"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(3))
                    .build()

                val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                if (response.statusCode() == 200) {
                    val gossip = objectMapper.readValue(response.body(), GossipMessage::class.java)
                    nodeRegistry.merge(gossip.nodes)
                    logger.info("[${nodeState.nodeName}] Joined via $seed, discovered ${gossip.nodes.size} nodes")
                    foundAnyPeer = true
                }
            }.onFailure {
                logger.warn("[${nodeState.nodeName}] Failed to join via $seed: ${it.message}")
            }
        }

        if (foundAnyPeer && nodeState.leaderId.get() == null) {
            discoverLeader()
        } else if (foundAnyPeer) {
            nodeState.leaderUrl.get()?.let { syncStateFromLeader(it) }
        }

        if (!foundAnyPeer) {
            logger.info("[${nodeState.nodeName}] No peers responded during join, starting election")
            electionService.startElection()
        }
    }

    private fun discoverLeader() {
        nodeRegistry.getPeerUrls().forEach { peer ->
            if (nodeState.leaderId.get() != null) return
            runCatching {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$peer/raft/status"))
                    .GET()
                    .timeout(Duration.ofSeconds(2))
                    .build()
                val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                if (response.statusCode() == 200) {
                    @Suppress("UNCHECKED_CAST")
                    val status = objectMapper.readValue(response.body(), Map::class.java) as Map<String, Any?>
                    val leaderId = status["leader"] as? String
                    val leaderUrl = status["leaderUrl"] as? String
                    val term = (status["term"] as? Number)?.toLong() ?: 0L
                    if (leaderId != null && leaderUrl != null) {
                        nodeState.becomeFollower(term, leaderId, leaderUrl)
                        logger.info("[${nodeState.nodeName}] Discovered leader: $leaderId at $leaderUrl (term=$term)")
                        syncStateFromLeader(leaderUrl)
                    }
                }
            }.onFailure {
                logger.warn("[${nodeState.nodeName}] Could not get status from $peer: ${it.message}")
            }
        }
    }

    private fun syncStateFromLeader(leaderUrl: String) {
        runCatching {
            val request = HttpRequest.newBuilder()
                .uri(URI.create("$leaderUrl/raft/snapshot"))
                .GET()
                .timeout(Duration.ofSeconds(5))
                .build()
            val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                val snapshot = objectMapper.readValue(response.body(), SnapshotResponse::class.java)
                lockRepository.loadSnapshot(snapshot.locks, snapshot.queue)
                logger.info(
                    "[${nodeState.nodeName}] State synced from leader $leaderUrl: " +
                        "${snapshot.locks.size} locks, ${snapshot.queue.size} queued"
                )
            } else {
                logger.warn("[${nodeState.nodeName}] Snapshot request to $leaderUrl returned ${response.statusCode()}")
            }
        }.onFailure {
            logger.warn("[${nodeState.nodeName}] Failed to sync state from leader $leaderUrl: ${it.message}")
        }
    }
}

