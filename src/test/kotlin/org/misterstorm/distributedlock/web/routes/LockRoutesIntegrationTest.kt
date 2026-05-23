package org.misterstorm.distributedlock.web.routes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.web.servlet.MockMvc
import tools.jackson.databind.ObjectMapper
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.repository.LockRepositoryInMemory
import java.time.LocalDateTime

@SpringBootTest
@AutoConfigureMockMvc
class LockRoutesIntegrationTest {
    @Autowired private lateinit var mvc: MockMvc
    @Autowired private lateinit var objectMapper: ObjectMapper
    @Autowired private lateinit var lockRepository: LockRepository
    @Autowired private lateinit var nodeState: NodeState
    @Autowired private lateinit var nodeRegistry: NodeRegistry

    @BeforeEach
    fun setup() {
        (lockRepository as LockRepositoryInMemory).clear()
        nodeState.becomeLeader()
        nodeRegistry.getPeerUrls().forEach { nodeRegistry.remove(it) }
    }

    @Test
    fun `should create new lock when resource is available`() {
        val candidate = lockCandidate(key = "resource-123", clientId = "client-001")
        mvc.http(objectMapper)
            .post("/lock")
            .withBody(candidate)
            .expectStatus(201)
            .expectJsonPath("$.key", "resource-123")
            .expectJsonPath("$.lockOwner", "client-001")
            .execute()
    }

    @Test
    fun `should return 202 when lock already exists for resource`() {
        val candidate = lockCandidate(key = "resource-456", clientId = "client-002")
        lockRepository.create(lock(key = "resource-456", lockOwner = "existing-owner"))
        mvc.http(objectMapper)
            .post("/lock")
            .withBody(candidate)
            .expectStatus(202)
            .execute()
    }

    @Test
    fun `should replace lock when existing lock is expired`() {
        val candidate = lockCandidate(key = "resource-789", clientId = "client-003")
        val expiredTime = LocalDateTime.now().minusSeconds(10)
        lockRepository.create(lock(key = "resource-789", lockOwner = "old-owner", expirationTime = expiredTime))
        mvc.http(objectMapper)
            .post("/lock")
            .withBody(candidate)
            .expectStatus(201)
            .expectJsonPath("$.key", "resource-789")
            .execute()
    }

    @Test
    fun `should return 201 when lock is successfully created for unique resource`() {
        val candidate = lockCandidate(key = "lock-1", clientId = "client-new")
        mvc.http(objectMapper)
            .post("/lock")
            .withBody(candidate)
            .expectStatus(201)
            .execute()
    }

    @Test
    fun `should return 204 when owner releases lock`() {
        val candidate = lockCandidate(key = "resource-delete-1", clientId = "client-delete-1")
        lockRepository.create(lock(key = "resource-delete-1", lockOwner = "client-delete-1"))
        mvc.http(objectMapper)
            .delete("/lock")
            .withBody(candidate)
            .expectStatus(204)
            .execute()
    }

    @Test
    fun `should return 404 when lock does not exist on release`() {
        val candidate = lockCandidate(key = "resource-not-found", clientId = "client-001")
        mvc.http(objectMapper)
            .delete("/lock")
            .withBody(candidate)
            .expectStatus(404)
            .execute()
    }

    @Test
    fun `should return 409 when requester is not lock owner on release`() {
        val candidate = lockCandidate(key = "resource-not-owner", clientId = "client-unauthorized")
        lockRepository.create(lock(key = "resource-not-owner", lockOwner = "lock-owner"))
        mvc.http(objectMapper)
            .delete("/lock")
            .withBody(candidate)
            .expectStatus(409)
            .execute()
    }

    @Test
    fun `should return renewed lock when owner requests renewal`() {
        val candidate = lockCandidate(key = "resource-renew-1", clientId = "client-renew-1")
        lockRepository.create(lock(key = "resource-renew-1", lockOwner = "client-renew-1"))
        mvc.http(objectMapper)
            .put("/lock")
            .withBody(candidate)
            .expectStatus(200)
            .expectJsonPath("$.key", "resource-renew-1")
            .execute()
    }

    @Test
    fun `should return 404 when lock does not exist on renewal`() {
        val candidate = lockCandidate(key = "resource-renew-not-found", clientId = "client-001")
        mvc.http(objectMapper)
            .put("/lock")
            .withBody(candidate)
            .expectStatus(404)
            .execute()
    }

    @Test
    fun `should return 409 when requester is not lock owner on renewal`() {
        val candidate = lockCandidate(key = "resource-renew-not-owner", clientId = "client-unauthorized")
        lockRepository.create(lock(key = "resource-renew-not-owner", lockOwner = "lock-owner"))
        mvc.http(objectMapper)
            .put("/lock")
            .withBody(candidate)
            .expectStatus(409)
            .execute()
    }

    @Test
    fun `should return LOCKED status when resource has active lock`() {
        val key = "resource-get-locked"
        lockRepository.create(lock(key = key, lockOwner = "client-lock-owner"))
        mvc.http(objectMapper)
            .get("/lock/$key")
            .expectStatus(200)
            .expectJsonPath("$.key", key)
            .expectJsonPath("$.status", "LOCKED")
            .execute()
    }

    @Test
    fun `should return AVAILABLE status when resource lock is expired`() {
        val key = "resource-get-not-locked"
        lockRepository.create(lock(key = key, lockOwner = "old-owner", expirationTime = LocalDateTime.now().minusSeconds(10)))
        mvc.http(objectMapper)
            .get("/lock/$key")
            .expectStatus(200)
            .expectJsonPath("$.key", key)
            .expectJsonPath("$.status", "AVAILABLE")
            .execute()
    }

    @Test
    fun `should return 404 when resource has no lock record`() {
        val key = "resource-get-not-found"
        mvc.http(objectMapper)
            .get("/lock/$key")
            .expectStatus(404)
            .execute()
    }

    @Test
    fun `should grant lock to queued candidate and enqueue new request when resource has waiting queue`() {
        val key = "resource-queue-test"
        val queuedClientId = "queued-client"
        val newClientId = "new-client"

        val expiredTime = LocalDateTime.now().minusSeconds(10)
        lockRepository.create(lock(key = key, lockOwner = "old-owner", expirationTime = expiredTime))

        lockRepository.addQueue(lock(key = key, lockOwner = queuedClientId))
        val candidate = lockCandidate(key = key, clientId = newClientId)
        mvc.http(objectMapper)
            .post("/lock")
            .withBody(candidate)
            .expectStatus(202)
            .execute()

        val createdLock = lockRepository.getByKey(key)
        assertNotNull(createdLock)
        assertEquals(queuedClientId, createdLock?.lockOwner)
        assertTrue(createdLock?.expirationTime?.isAfter(LocalDateTime.now()) ?: false)
        assertTrue(lockRepository.hasKeyInQueue(key))
    }
}
