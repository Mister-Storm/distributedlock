package org.misterstorm.distributedlock.web.routes

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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
import java.time.LocalDateTime

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
    fun `should transition to follower when heartbeat has valid term`() {
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

        assertEquals(Role.FOLLOWER, nodeState.role.get())
        assertEquals(5L, nodeState.currentTerm.get())
        assertEquals("leader-node", nodeState.leaderId.get())
        assertEquals("http://leader:8080", nodeState.leaderUrl.get())
    }

    @Test
    fun `should return 400 when heartbeat has stale term`() {
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

        assertEquals(10L, nodeState.currentTerm.get())
    }

    @Test
    fun `should return 400 when node is already leader and receives heartbeat`() {
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

        assertTrue(nodeState.isLeader())
    }

    @Test
    fun `should apply pending entries when heartbeat contains recent commits`() {
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

        assertFalse(lockRepository.hasPending(idempotencyKey))
        assertNotNull(lockRepository.getByKey("resource-hb-1"))
    }

    @Test
    fun `should ignore heartbeat when recent commit key is unknown`() {
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
    fun `should grant vote when node has not voted yet in current term`() {
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

        assertTrue(nodeRegistry.getPeerUrls().contains("http://candidate-a:8080"))
    }

    @Test
    fun `should grant vote when already voted for same candidate`() {
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
    fun `should deny vote when node already voted for different candidate`() {
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
    fun `should deny vote when request has stale term`() {
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
    fun `should return current term when vote request has stale term`() {
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
    fun `should merge nodes into registry when gossip message is received`() {
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

        assertTrue(nodeRegistry.getPeerUrls().contains("http://node2:8081"))
        assertTrue(nodeRegistry.getPeerUrls().contains("http://node3:8082"))
    }

    @Test
    fun `should include own node in gossip response when peer gossips`() {
        mvc.http(objectMapper)
            .post("/raft/gossip")
            .withBody(GossipMessage(mapOf("peer" to "http://peer:9090")))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `should return own node when gossip is received with empty nodes`() {
        mvc.http(objectMapper)
            .post("/raft/gossip")
            .withBody(GossipMessage(emptyMap()))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `should save pending entry when replicate request is received`() {
        val idempotencyKey = "replicate-key-001"
        val lockEntry = lock(key = "resource-rep-1", lockOwner = "owner-1")

        mvc.http(objectMapper)
            .post("/raft/replicate")
            .withBody(ReplicateRequest(idempotencyKey, LockOperation.CREATE, lockEntry))
            .expectStatus(200)
            .execute()

        assertTrue(lockRepository.hasPending(idempotencyKey))
    }

    @Test
    fun `should be idempotent when replicate is called with already pending key`() {
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
    fun `should store all operations when multiple replicate requests are received`() {
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

        assertTrue(lockRepository.hasPending(createKey))
        assertTrue(lockRepository.hasPending(releaseKey))
    }

    @Test
    fun `should create lock and clear pending when commit is received for CREATE operation`() {
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

        assertFalse(lockRepository.hasPending(idempotencyKey))
        assertNotNull(lockRepository.getByKey("resource-commit-1"))
    }

    @Test
    fun `should remove lock and clear pending when commit is received for RELEASE operation`() {
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

        assertFalse(lockRepository.hasPending(idempotencyKey))
        assertNull(lockRepository.getByKey("resource-release-1"))
    }

    @Test
    fun `should ignore commit when idempotency key is unknown`() {
        mvc.http(objectMapper)
            .post("/raft/commit")
            .withBody(CommitRequest("non-existent-key"))
            .expectStatus(200)
            .execute()
    }

    @Test
    fun `should return node information when status is requested`() {
        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .expectJsonPath("$.node", "node1")
            .expectJsonPath("$.url", "http://localhost:8080")
            .execute()
    }

    @Test
    fun `should return LEADER role when node is leader and status is requested`() {
        nodeState.becomeLeader()

        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .expectJsonPath("$.role", "LEADER")
            .expectJsonPath("$.node", "node1")
            .execute()
    }

    @Test
    fun `should return FOLLOWER role and known leader when status is requested`() {
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
    fun `should include existing locks when status is requested`() {
        lockRepository.create(lock(key = "status-lock-key", lockOwner = "status-owner"))

        mvc.http(objectMapper)
            .get("/raft/status")
            .expectStatus(200)
            .execute()

        assertTrue(lockRepository.getAllLocks().any { it.key == "status-lock-key" })
    }

    @Test
    fun `should register node and return gossip when join is requested`() {
        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "new-node", url = "http://new-node:9090"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.new-node", "http://new-node:9090")
            .execute()

        assertTrue(nodeRegistry.getPeerUrls().contains("http://new-node:9090"))
    }

    @Test
    fun `should include self node in gossip when new node joins`() {
        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "joining-node", url = "http://joining:7070"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.node1", "http://localhost:8080")
            .expectJsonPath("$.nodes.joining-node", "http://joining:7070")
            .execute()
    }

    @Test
    fun `should update node url when joining node already exists in registry`() {
        nodeRegistry.merge(mapOf("existing-node" to "http://existing:1111"))

        mvc.http(objectMapper)
            .post("/raft/join")
            .withBody(JoinRequest(name = "existing-node", url = "http://existing:2222"))
            .expectStatus(200)
            .expectJsonPath("$.nodes.existing-node", "http://existing:2222")
            .execute()

        assertTrue(nodeRegistry.getPeerUrls().contains("http://existing:2222"))
        assertFalse(nodeRegistry.getPeerUrls().contains("http://existing:1111"))
    }

    @Test
    fun `should return exclude false when suspect node is reachable`() {
        mockServer = ClientAndServer.startClientAndServer()
        val port = mockServer?.port
        mockServer
            ?.`when`(request().withMethod("GET").withPath("/raft/status"))
            ?.respond(response().withStatusCode(200))

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$port"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", false)
            .execute()
    }

    @Test
    fun `should return exclude true when suspect node returns non 200 status`() {
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
    fun `should return exclude true when suspect node is unreachable`() {
        val freePort = ServerSocket(0).use { it.localPort }

        mvc.http(objectMapper)
            .post("/raft/exclude-vote")
            .withBody(ExcludeVoteRequest(suspectUrl = "http://localhost:$freePort"))
            .expectStatus(200)
            .expectJsonPath("$.exclude", true)
            .execute()
    }

    @Test
    fun `should return exclude true when suspect node times out`() {
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

    @Test
    fun `should return 403 when snapshot is requested from non-leader node`() {
        nodeState.becomeFollower(1L, "other-leader", "http://other:8080")

        mvc.http(objectMapper)
            .get("/raft/snapshot")
            .expectStatus(403)
            .execute()
    }

    @Test
    fun `should return empty snapshot when leader has no locks or queued entries`() {
        nodeState.becomeLeader()

        mvc.http(objectMapper)
            .get("/raft/snapshot")
            .expectStatus(200)
            .execute()

        assertTrue(lockRepository.getAllLocks().isEmpty())
        assertTrue(lockRepository.getAllInQueue().isEmpty())
    }

    @Test
    fun `should return all active locks in snapshot when leader has active locks`() {
        nodeState.becomeLeader()
        lockRepository.create(lock(key = "snap-lock-1", lockOwner = "owner-1"))
        lockRepository.create(lock(key = "snap-lock-2", lockOwner = "owner-2"))

        mvc.http(objectMapper)
            .get("/raft/snapshot")
            .expectStatus(200)
            .execute()

        val locks = lockRepository.getAllLocks()
        assertTrue(locks.any { it.key == "snap-lock-1" })
        assertTrue(locks.any { it.key == "snap-lock-2" })
    }

    @Test
    fun `should exclude expired locks from snapshot when leader has expired locks`() {
        nodeState.becomeLeader()
        lockRepository.create(lock(key = "snap-active", lockOwner = "owner-active"))
        lockRepository.create(
            lock(key = "snap-expired", lockOwner = "owner-expired", expirationTime = LocalDateTime.now().minusSeconds(10))
        )

        mvc.http(objectMapper)
            .get("/raft/snapshot")
            .expectStatus(200)
            .expectJsonPath("$.locks[0].key", "snap-active")
            .execute()

        assertEquals(2, lockRepository.getAllLocks().size)
    }

    @Test
    fun `should include queued entries in snapshot when leader has locks in queue`() {
        nodeState.becomeLeader()
        lockRepository.create(lock(key = "snap-queued-res", lockOwner = "active-owner"))
        lockRepository.addQueue(lock(key = "snap-queued-res", lockOwner = "waiting-owner"))

        mvc.http(objectMapper)
            .get("/raft/snapshot")
            .expectStatus(200)
            .execute()

        val queue = lockRepository.getAllInQueue()
        assertTrue(queue.any { it.key == "snap-queued-res" && it.lockOwner == "waiting-owner" })
    }
}
