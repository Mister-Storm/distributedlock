package org.misterstorm.distributedlock.web.routes

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.Role
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.services.GossipMessage
import org.misterstorm.distributedlock.infra.raft.services.NodeJoinService
import org.misterstorm.distributedlock.infra.raft.services.SnapshotResponse
import org.misterstorm.distributedlock.infra.repository.LockRepositoryInMemory
import org.mockserver.integration.ClientAndServer
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.mockserver.model.MediaType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.DefaultApplicationArguments
import org.springframework.boot.test.context.SpringBootTest
import tools.jackson.databind.ObjectMapper
import java.time.LocalDateTime

@SpringBootTest
class NodeJoinServiceIntegrationTest {

    @Autowired private lateinit var nodeJoinService: NodeJoinService
    @Autowired private lateinit var lockRepository: LockRepository
    @Autowired private lateinit var nodeState: NodeState
    @Autowired private lateinit var nodeRegistry: NodeRegistry
    @Autowired private lateinit var objectMapper: ObjectMapper

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
        nodeRegistry.getPeerUrls().forEach { nodeRegistry.remove(it) }
        nodeRegistry.clearPendingRemovals()
    }

    @Test
    fun `should sync locks from leader when joining cluster with active state`() {
        val server = ClientAndServer.startClientAndServer().also { mockServer = it }
        val leaderUrl = "http://localhost:${server.port}"

        val activeLock = lock(key = "join-lock-1", lockOwner = "owner-1")
        val gossipBody = objectMapper.writeValueAsString(GossipMessage(mapOf("mock-leader" to leaderUrl)))
        val statusBody = objectMapper.writeValueAsString(
            mapOf("leader" to "mock-leader", "leaderUrl" to leaderUrl, "term" to 1)
        )
        val snapshotBody = objectMapper.writeValueAsString(
            SnapshotResponse(locks = listOf(activeLock), queue = emptyList())
        )

        server
            .`when`(request().withMethod("POST").withPath("/raft/join"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(gossipBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/status"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(statusBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/snapshot"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(snapshotBody))

        nodeRegistry.merge(mapOf("mock-leader" to leaderUrl))
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertNotNull(lockRepository.getByKey("join-lock-1"))
        assertEquals("owner-1", lockRepository.getByKey("join-lock-1")?.lockOwner)
    }

    @Test
    fun `should sync queue from leader when leader has queued locks`() {
        val server = ClientAndServer.startClientAndServer().also { mockServer = it }
        val leaderUrl = "http://localhost:${server.port}"

        val queuedLock = lock(key = "join-queued-res", lockOwner = "waiting-owner")
        val gossipBody = objectMapper.writeValueAsString(GossipMessage(mapOf("mock-leader" to leaderUrl)))
        val statusBody = objectMapper.writeValueAsString(
            mapOf("leader" to "mock-leader", "leaderUrl" to leaderUrl, "term" to 1)
        )
        val snapshotBody = objectMapper.writeValueAsString(
            SnapshotResponse(locks = emptyList(), queue = listOf(queuedLock))
        )

        server
            .`when`(request().withMethod("POST").withPath("/raft/join"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(gossipBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/status"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(statusBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/snapshot"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(snapshotBody))

        nodeRegistry.merge(mapOf("mock-leader" to leaderUrl))
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertTrue(lockRepository.hasKeyInQueue("join-queued-res"))
    }

    @Test
    fun `should become leader when no peers respond during join`() {
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertEquals(Role.LEADER, nodeState.role.get())
    }

    @Test
    fun `should not load expired locks when snapshot contains expired entries`() {
        val server = ClientAndServer.startClientAndServer().also { mockServer = it }
        val leaderUrl = "http://localhost:${server.port}"

        val activeLock = lock(key = "join-active", lockOwner = "owner-active")
        val expiredLock = lock(
            key = "join-expired",
            lockOwner = "owner-expired",
            expirationTime = LocalDateTime.now().minusSeconds(30)
        )
        val gossipBody = objectMapper.writeValueAsString(GossipMessage(mapOf("mock-leader" to leaderUrl)))
        val statusBody = objectMapper.writeValueAsString(
            mapOf("leader" to "mock-leader", "leaderUrl" to leaderUrl, "term" to 1)
        )
        val snapshotBody = objectMapper.writeValueAsString(
            SnapshotResponse(locks = listOf(activeLock, expiredLock), queue = emptyList())
        )

        server
            .`when`(request().withMethod("POST").withPath("/raft/join"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(gossipBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/status"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(statusBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/snapshot"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(snapshotBody))

        nodeRegistry.merge(mapOf("mock-leader" to leaderUrl))
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertNotNull(lockRepository.getByKey("join-active"))
        assertNull(lockRepository.getByKey("join-expired"))
    }

    @Test
    fun `should not crash and keep empty state when snapshot endpoint returns 403`() {
        val server = ClientAndServer.startClientAndServer().also { mockServer = it }
        val leaderUrl = "http://localhost:${server.port}"

        val gossipBody = objectMapper.writeValueAsString(GossipMessage(mapOf("mock-leader" to leaderUrl)))
        val statusBody = objectMapper.writeValueAsString(
            mapOf("leader" to "mock-leader", "leaderUrl" to leaderUrl, "term" to 1)
        )

        server
            .`when`(request().withMethod("POST").withPath("/raft/join"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(gossipBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/status"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(statusBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/snapshot"))
            .respond(response().withStatusCode(403))

        nodeRegistry.merge(mapOf("mock-leader" to leaderUrl))
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertTrue(lockRepository.getAllLocks().isEmpty())
    }

    @Test
    fun `should become follower with correct term when leader is discovered during join`() {
        val server = ClientAndServer.startClientAndServer().also { mockServer = it }
        val leaderUrl = "http://localhost:${server.port}"

        val gossipBody = objectMapper.writeValueAsString(GossipMessage(mapOf("mock-leader" to leaderUrl)))
        val statusBody = objectMapper.writeValueAsString(
            mapOf("leader" to "mock-leader", "leaderUrl" to leaderUrl, "term" to 7)
        )
        val snapshotBody = objectMapper.writeValueAsString(
            SnapshotResponse(locks = emptyList(), queue = emptyList())
        )

        server
            .`when`(request().withMethod("POST").withPath("/raft/join"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(gossipBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/status"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(statusBody))
        server
            .`when`(request().withMethod("GET").withPath("/raft/snapshot"))
            .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(snapshotBody))

        nodeRegistry.merge(mapOf("mock-leader" to leaderUrl))
        nodeJoinService.run(DefaultApplicationArguments(*arrayOf<String>()))

        assertEquals(Role.FOLLOWER, nodeState.role.get())
        assertEquals(7L, nodeState.currentTerm.get())
        assertEquals("mock-leader", nodeState.leaderId.get())
    }
}

