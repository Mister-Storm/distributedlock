package org.misterstorm.distributedlock.web.routes

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteRequest
import org.misterstorm.distributedlock.infra.raft.services.CommitRequest
import org.misterstorm.distributedlock.infra.raft.services.ExcludeVoteRequest
import org.misterstorm.distributedlock.infra.raft.services.GossipMessage
import org.misterstorm.distributedlock.infra.raft.services.JoinRequest
import org.misterstorm.distributedlock.infra.raft.services.ReplicateRequest
import org.misterstorm.distributedlock.infra.repository.LockRepositoryInMemory
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.web.servlet.MockMvc
import tools.jackson.databind.ObjectMapper
import java.net.ServerSocket

@SpringBootTest
@AutoConfigureMockMvc
class RaftRoutesIntegrationTest {

    @Autowired private lateinit var mvc: MockMvc
    @Autowired private lateinit var objectMapper: ObjectMapper
    @Autowired private lateinit var lockRepository: LockRepository
    @Autowired private lateinit var nodeState: NodeState
    @Autowired private lateinit var nodeRegistry: NodeRegistry

    private var mockServer: ClientAndServer? = null

    @BeforeEach
    fun setup() {
        (lockRepository as LockRepositoryInMemory).clear()
        nodeState.becomeFollower(0L, null, null)
        nodeRegistry.getPeerUrls().forEach { nodeRegistry.remove(it) }
        nodeRegistry.clearPendingRemovals()
    }

    @AfterEach
    fun tearDown() {
        mockServer?.stop()
        mockServer = null
    }

    @Test
    fun `heartbeat with valid term transitions node to follower`() {
        mvc.http(objectMapper)
            .post("/raft/heartbeat")
            .withBody(
                HeartbeatRequest(
                    leaderName = "leader-node",
                    leaderUrl = "http://leader:8080",
                    term = 5L
                )
            )
            .expectStatus(200)
            .execute()

        assert(nodeState.role.get() == Role.FOLLOWER) {
            "Expected node role to be FOLLOWER but was ${nodeState.role.get()}"
        }
        assert(nodeState.currentTerm.get() == 5L) {
            "Expected term to be 5 but was ${nodeState.currentTerm.get()}"
        }
        assert(nodeState.leaderId.get() == "leader-node") {
            "Expected leaderId to be 'leader-node' but was ${nodeState.leaderId.get()}"
        }
        assert(nodeState.leaderUrl.get() == "http://leader:8080") {
            "Expected leaderUrl to be 'http://leader:8080' but was ${nodeState.leaderUrl.get()}"
        }
    }

    @Test
    fun `heartbeat with stale term returns 400`() {
        nodeState.becomeFollower(10L, "current-leader", "http://current:8080")

        mvc.http(objectMapper)
            .post("/raft/heartbeat")
            .withBody(
                HeartbeatRequest(
                    leaderName = "old-leader",
                    leaderUrl = "http://old:8080",
                    term = 5L
                )
            )
            .expectStatus(400)
            .execute()

        assert(nodeState.currentTerm.get() == 10L) {
            "Expected term to remain 10 but was ${nodeState.currentTerm.get()}"
        }
    }

    @Test
    fun `heartbeat when node is leader returns 400`() {
        nodeState.becomeLeader()

        mvc.http(objectMapper)
            .post("/raft/heartbeat")
            .withBody(
                HeartbeatRequest(
                    leaderName = "other-leader",
                    leaderUrl = "http://other:8080",
                    term = 1L
                )
            )
            .expectStatus(400)
            .execute()

        assert(nodeState.isLeader()) { "Node should remain leader after rejecting heartbeat" }
    }

    @Test
    fun `heartbeat with recentCommits applies pending entries`() {
        val idempotencyKey = "heartbeat-commit-key"
        lockRepository.savePending(
            replicaEntry(
                idempotencyKey = idempotencyKey,
                operation = LockOperation.CREATE,
                lock = lock(key = "resource-hb-1", lockOwner = "owner-hb")
            )
        )

        mvc.http(objectMapper)
            .post("/raft/heartbeat")
            .withBody(
                HeartbeatRequest(
                    leaderName = "leader-node",
                    leaderUrl = "http://leader:8080",
                    term = 3L,
                    recentCommits = listOf(idempotencyKey)
                )
            )
            .expectStatus(200)
            .execute()

        assert(!lockRepository.hasPending(idempotencyKey)) {
            "Expected pending entry '$idempotencyKey' to be committed after heartbeat"
        }
        assert(lockRepository.getByKey("resource-hb-1") != null) {
            "Expected lock 'resource-hb-1' to exist after commit via heartbeat"
        }
    }

    @Test
    fun `heartbeat with unknown recentCommits key is silently ignored`() {
        mvc.http(objectMapper)
            .post("/raft/heartbeat")
            .withBody(
                HeartbeatRequest(
                    leaderName = "leader-node",
                    leaderUrl = "http://leader:8080",
                    term = 2L,
                    recentCommits = listOf("non-existent-idempotency-key")
                )
            )
            .expectStatus(200)
            .execute()
    }

    @Test
    fun `vote granted when node has not voted yet`() {
        mvc.http(objectMapper)
            .post("/raft/vote")
            .withBody(
                VoteRequest(
                    candidateName = "candidate-A",
                    candidateUrl = "http://candidate-a:8080",
                    term = 1L
                )
            )
            .expectStatus(200)
            .expectJsonPath("$.voteGranted", true)
            .execute()

        assert(nodeRegistry.getPeerUrls().contains("http://candidate-a:8080")) {
            "Expected candidate URL to be registered in the node registry after vote"
        }
    }

    @Test
    fun `vote granted when already voted for the same candidate`() {
        nodeState.voteFor("candidate-A")

        mvc.http(objectMapper)
            .post("/raft/vote")
            .withBody(
                VoteRequest(
                    candidateName = "candidate-A",
                    candidateUrl = "http://candidate-a:8080",
                    term = 1L
                )
            )
            .expectStatus(200)
            .expectJsonPath("$.voteGranted", true)
            .execute()
    }

    @Test
    fun `vote denied when node already voted for a different candidate`() {
        nodeState.voteFor("candidate-B")

        mvc.http(objectMapper)
            .post("/raft/vote")
            .withBody(
                VoteRequest(
                    candidateName = "candidate-A",
                    candidateUrl = "http://candidate-a:8080",
                    term = 1L
                )
            )
            .expectStatus(200)
            .expectJsonPath("$.voteGranted", false)
            .execute()
    }

    @Test
    fun `vote denied when request has stale term`() {
        nodeState.becomeFollower(10L, null, null)

        mvc.http(objectMapper)
            .post("/raft/vote")
            .withBody(
                VoteRequest(
                    candidateName = "candidate-A",
                    candidateUrl = "http://candidate-a:8080",
                    term = 5L
                )
            )
            .expectStatus(200)
            .expectJsonPath("$.voteGranted", false)
            .execute()
    }

    @Test
    fun `vote response includes current term on stale request`() {
        nodeState.becomeFollower(10L, null, null)

        mvc.http(objectMapper)
            .post("/raft/vote")
            .withBody(
                VoteRequest(
                    candidateName = "candidate-A",
                    candidateUrl = "http://candidate-a:8080",
                    term = 5L
                )
            )
            .expectStatus(200)
            .expectJsonPath("$.term", 10)
            .expectJsonPath("$.voteGranted", false)
            .execute()
    }


    @Test
    fun `gossip merges received nodes into registry and returns all nodes`() {
        val incomingNodes = mapOf(
            "node2" to "http://node2:8081",
            "node3" to "http://node3:8082"
        )

        mvc.http(objectMapper)
            .post("/raft/gossip")
            .withBody(GossipMessage(incomingNodes))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node2", "http://node2:8081")
            .expectJsonPath("$.nodes.node3", "http://node3:8082")
            .execute()

        assert(nodeRegistry.getPeerUrls().contains("http://node2:8081")) {
            "Expected node2 URL in peer list after gossip"
        }
        assert(nodeRegistry.getPeerUrls().contains("http://node3:8082")) {
            "Expected node3 URL in peer list after gossip"
        }
    }

    @Test
    fun `gossip response includes own node`() {
        mvc.http(objectMapper)
            .post("/raft/gossip")
            .withBody(GossipMessage(mapOf("peer" to "http://peer:9090")))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `gossip with empty nodes returns own node`() {
        mvc.http(objectMapper)
            .post("/raft/gossip")
            .withBody(GossipMessage(emptyMap()))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `replicate saves new pending entry and returns 200`() {
        val idempotencyKey = "replicate-key-001"
        val lockEntry = lock(key = "resource-rep-1", lockOwner = "owner-1")

        mvc.http(objectMapper)
            .post("/raft/replicate")
            .withBody(ReplicateRequest(idempotencyKey, LockOperation.CREATE, lockEntry))
            .expectStatus(200)
            .execute()

        assert(lockRepository.hasPending(idempotencyKey)) {
            "Expected pending entry '$idempotencyKey' to be saved after /raft/replicate"
        }
    }

    @Test
    fun `replicate is idempotent for an existing pending key`() {
        val idempotencyKey = "replicate-key-idempotent"
        val lockEntry = lock(key = "resource-idem", lockOwner = "owner-idem")
        lockRepository.savePending(
            replicaEntry(idempotencyKey = idempotencyKey, lock = lockEntry)
        )

        mvc.http(objectMapper)
            .post("/raft/replicate")
            .withBody(ReplicateRequest(idempotencyKey, LockOperation.CREATE, lockEntry))
            .expectStatus(200)
            .execute()
    }

    @Test
    fun `replicate stores different operations correctly`() {
        val createKey = "rep-create-key"
        val releaseKey = "rep-release-key"
        val lockEntry = lock(key = "resource-ops", lockOwner = "owner-ops")

        mvc.http(objectMapper)
            .post("/raft/replicate")
            .withBody(ReplicateRequest(createKey, LockOperation.CREATE, lockEntry))
            .expectStatus(200)
            .execute()

        mvc.http(objectMapper)
            .post("/raft/replicate")
            .withBody(ReplicateRequest(releaseKey, LockOperation.RELEASE, lockEntry))
            .expectStatus(200)
            .execute()

        assert(lockRepository.hasPending(createKey)) { "Expected CREATE pending entry" }
        assert(lockRepository.hasPending(releaseKey)) { "Expected RELEASE pending entry" }
    }

    @Test
    fun `commit applies pending CREATE entry and removes it from pending`() {
        val idempotencyKey = "commit-create-001"
        val lockEntry = lock(key = "resource-commit-1", lockOwner = "owner-commit-1")
        lockRepository.savePending(
            replicaEntry(
                idempotencyKey = idempotencyKey,
                operation = LockOperation.CREATE,
                lock = lockEntry
            )
        )

        mvc.http(objectMapper)
            .post("/raft/commit")
            .withBody(CommitRequest(idempotencyKey))
            .expectStatus(200)
            .execute()

        assert(!lockRepository.hasPending(idempotencyKey)) {
            "Expected pending entry to be removed after commit"
        }
        assert(lockRepository.getByKey("resource-commit-1") != null) {
            "Expected lock to exist in store after CREATE commit"
        }
    }

    @Test
    fun `commit applies pending RELEASE entry removing the lock`() {
        val idempotencyKey = "commit-release-001"
        val lockEntry = lock(key = "resource-release-1", lockOwner = "owner-release-1")
        lockRepository.create(lockEntry)
        lockRepository.savePending(
            replicaEntry(
                idempotencyKey = idempotencyKey,
                operation = LockOperation.RELEASE,
                lock = lockEntry
            )
        )

        mvc.http(objectMapper)
            .post("/raft/commit")
            .withBody(CommitRequest(idempotencyKey))
            .expectStatus(200)
            .execute()

        assert(!lockRepository.hasPending(idempotencyKey)) {
            "Expected pending entry to be removed after commit"
        }
        assert(lockRepository.getByKey("resource-release-1") == null) {
            "Expected lock to be removed from store after RELEASE commit"
        }
    }

    @Test
    fun `commit with unknown idempotency key is silently ignored`() {
        mvc.http(objectMapper)
            .post("/raft/commit")
            .withBody(CommitRequest("non-existent-key"))
            .expectStatus(200)
            .execute()
    }

    @Test
    fun `status returns current node information`() {
        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .expectJsonPath("$.node", "node1")
            .expectJsonPath("$.url", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `status reflects leader role when node is leader`() {
        nodeState.becomeLeader()

        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .expectJsonPath("$.role", "LEADER")
            .expectJsonPath("$.node", "node1")
            .execute()
    }

    @Test
    fun `status reflects follower role and known leader`() {
        nodeState.becomeFollower(5L, "leader-node", "http://leader:8080")

        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .expectJsonPath("$.role", "FOLLOWER")
            .expectJsonPath("$.term", 5)
            .expectJsonPath("$.leader", "leader-node")
            .expectJsonPath("$.leaderUrl", "http://leader:8080")
            .execute()
    }

    @Test
    fun `status includes existing locks`() {
        lockRepository.create(lock(key = "status-lock-key", lockOwner = "status-owner"))

        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .execute()

        assert(lockRepository.getAllLocks().any { it.key == "status-lock-key" }) {
            "Expected lock to appear in status"
        }
    }

    @Test
    fun `join adds new node to registry and returns gossip message`() {
        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "new-node", url = "http://new-node:9090"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.new-node", "http://new-node:9090")
            .execute()

        assert(nodeRegistry.getPeerUrls().contains("http://new-node:9090")) {
            "Expected 'new-node' URL to be present in the registry after join"
        }
    }

    @Test
    fun `join response includes the self node as well`() {
        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "joining-node", url = "http://joining:7070"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .expectJsonPath("$.nodes.joining-node", "http://joining:7070")
            .execute()
    }

    @Test
    fun `join with repeated name updates the url in registry`() {
        nodeRegistry.merge(mapOf("existing-node" to "http://existing:1111"))

        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "existing-node", url = "http://existing:2222"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.existing-node", "http://existing:2222")
            .execute()

        assert(nodeRegistry.getPeerUrls().contains("http://existing:2222")) {
            "Expected updated URL for 'existing-node'"
        }
        assert(!nodeRegistry.getPeerUrls().contains("http://existing:1111")) {
            "Expected old URL for 'existing-node' to be replaced"
        }
    }

    @Test
    fun `excludeVote returns exclude=false when suspect node responds with 200`() {
        mockServer = ClientAndServer.startClientAndServer()
        val port = mockServer?.port
        mockServer
            ?. `when`(request().withMethod("GET").withPath("/raft/status"))
            ?.respond(response().withStatusCode(200))

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$port"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", false)
            .execute()
    }

    @Test
    fun `excludeVote returns exclude=true when suspect node returns non-200`() {
        mockServer = ClientAndServer.startClientAndServer()
        val port = mockServer?.port
        mockServer
            ?.`when`(request().withMethod("GET").withPath("/raft/status"))
            ?.respond(response().withStatusCode(503))

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$port"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", true)
            .execute()
    }

    @Test
    fun `excludeVote returns exclude=true when suspect node is unreachable`() {
        val freePort = ServerSocket(0).use { it.localPort }

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$freePort"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", true)
            .execute()
    }

    @Test
    fun `excludeVote returns exclude=true when suspect node times out`() {
        mockServer = ClientAndServer.startClientAndServer()
        val port = mockServer?.port
        mockServer
            ?.`when`(request().withMethod("GET").withPath("/raft/status"))
            ?.respond(
                response()
                    .withStatusCode(200)
                    .withDelay(
                        org.mockserver.model.Delay(
                            java.util.concurrent.TimeUnit.SECONDS,
                            4
                        )
                    )
            )

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$port"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", true)
            .execute()
    }
}

