package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.slf4j.LoggerFactory
import org.slf4j.MDC
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
        MDC.put("node", nodeState.nodeName)
        val seeds = nodeRegistry.getPeerUrls()

        if (seeds.isEmpty()) {
            logger.info("No seeds configured, starting election immediately")
            MDC.remove("node")
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
                MDC.put("seed", seed)
                if (response.statusCode() == 200) {
                    val gossip = objectMapper.readValue(response.body(), GossipMessage::class.java)
                    nodeRegistry.merge(gossip.nodes)
                    MDC.put("discoveredNodes", gossip.nodes.size.toString())
                    logger.info("Joined cluster via seed")
                    MDC.remove("discoveredNodes")
                    foundAnyPeer = true
                } else {
                    MDC.put("statusCode", response.statusCode().toString())
                    logger.warn("Join request rejected by seed")
                    MDC.remove("statusCode")
                }
                MDC.remove("seed")
            }.onFailure { ex ->
                MDC.put("seed", seed)
                MDC.put("error", ex.message)
                logger.warn("Failed to join via seed")
                MDC.remove("seed"); MDC.remove("error")
            }
        }

        if (foundAnyPeer && nodeState.leaderId.get() == null) {
            discoverLeader()
        } else if (foundAnyPeer) {
            nodeState.leaderUrl.get()?.let { syncStateFromLeader(it) }
        }

        if (!foundAnyPeer) {
            logger.info("No peers responded during join, starting election")
        }

        MDC.remove("node")

        if (!foundAnyPeer) {
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
                MDC.put("peer", peer)
                if (response.statusCode() == 200) {
                    @Suppress("UNCHECKED_CAST")
                    val status = objectMapper.readValue(response.body(), Map::class.java) as Map<String, Any?>
                    val leaderId = status["leader"] as? String
                    val leaderUrl = status["leaderUrl"] as? String
                    val term = (status["term"] as? Number)?.toLong() ?: 0L
                    if (leaderId != null && leaderUrl != null) {
                        nodeState.becomeFollower(term, leaderId, leaderUrl)
                        MDC.put("leader", leaderId)
                        MDC.put("leaderUrl", leaderUrl)
                        MDC.put("term", term.toString())
                        logger.info("Leader discovered")
                        MDC.remove("leader"); MDC.remove("leaderUrl"); MDC.remove("term")
                        MDC.remove("peer")
                        syncStateFromLeader(leaderUrl)
                        return@forEach
                    }
                } else {
                    MDC.put("statusCode", response.statusCode().toString())
                    logger.warn("Status request to peer failed")
                    MDC.remove("statusCode")
                }
                MDC.remove("peer")
            }.onFailure { ex ->
                MDC.put("peer", peer)
                MDC.put("error", ex.message)
                logger.warn("Could not get status from peer")
                MDC.remove("peer"); MDC.remove("error")
            }
        }
    }

    private fun syncStateFromLeader(leaderUrl: String) {
        MDC.put("leaderUrl", leaderUrl)
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
                MDC.put("locks", snapshot.locks.size.toString())
                MDC.put("queued", snapshot.queue.size.toString())
                logger.info("State synced from leader")
                MDC.remove("locks"); MDC.remove("queued")
            } else {
                MDC.put("statusCode", response.statusCode().toString())
                logger.warn("Snapshot request to leader failed")
                MDC.remove("statusCode")
            }
        }.onFailure { ex ->
            MDC.put("error", ex.message)
            logger.warn("Failed to sync state from leader")
            MDC.remove("error")
        }
        MDC.remove("leaderUrl")
    }
}
