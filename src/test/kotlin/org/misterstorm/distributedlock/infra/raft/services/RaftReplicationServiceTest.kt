package org.misterstorm.distributedlock.infra.raft.services

import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.infra.raft.models.NodeRegistry
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.jsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.net.http.HttpClient
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RaftReplicationServiceTest {

    private val objectMapper: ObjectMapper = jsonMapper { addModule(kotlinModule()) }

    private fun mockHttpResponse(statusCode: Int): HttpResponse<String> {
        val response = mockk<HttpResponse<String>>()
        every { response.statusCode() } returns statusCode
        return response
    }

    private fun createSut(
        peers: List<String> = emptyList(),
        heartbeatService: HeartbeatService = mockk(relaxed = true),
        httpClient: HttpClient = mockk(),
    ): Triple<RaftReplicationService, HeartbeatService, HttpClient> {
        val nodeRegistry = mockk<NodeRegistry>()
        every { nodeRegistry.getPeerUrls() } returns peers
        return Triple(
            RaftReplicationService(nodeRegistry, heartbeatService, httpClient, objectMapper),
            heartbeatService,
            httpClient,
        )
    }

    @Test
    fun `should return true immediately when there are no peers`() {
        val (sut, heartbeatService, _) = createSut(peers = emptyList())

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 0) { heartbeatService.recordCommit(any()) } },
        )
    }

    @Test
    fun `should return true and record commit when all peers acknowledge`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())
        justRun { heartbeatService.recordCommit(any()) }

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 1) { heartbeatService.recordCommit(any()) } },
            { verify(exactly = peers.size) { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } },
        )
    }

    @Test
    fun `should return true with a single peer that acknowledges`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())
        justRun { heartbeatService.recordCommit(any()) }

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.RELEASE, createLock())

        assertTrue(result)
    }

    @Test
    fun `should return true when quorum is reached even if not all peers acknowledge`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082", "http://peer3:8083")

        every { httpClient.send(match { it.uri().host == "peer1" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.send(match { it.uri().host == "peer2" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.send(match { it.uri().host == "peer3" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())
        justRun { heartbeatService.recordCommit(any()) }

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 1) { heartbeatService.recordCommit(any()) } },
        )
    }

    @Test
    fun `should return false when no peer acknowledges and quorum is not reached`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { heartbeatService.recordCommit(any()) } },
            { verify(exactly = 0) { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } },
        )
    }

    @Test
    fun `should return false when there is only one peer and it does not acknowledge`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(500)

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.RENEW, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { heartbeatService.recordCommit(any()) } },
        )
    }

    @Test
    fun `should count peer as nack when HTTP call throws an exception`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } throws java.io.IOException("Connection refused")

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { heartbeatService.recordCommit(any()) } },
        )
    }

    @Test
    fun `should still reach quorum when some peers throw and others ack`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082", "http://peer3:8083")

        every { httpClient.send(match { it.uri().host == "peer1" }, any<HttpResponse.BodyHandler<String>>()) } throws java.io.IOException("timeout")
        every { httpClient.send(match { it.uri().host == "peer2" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.send(match { it.uri().host == "peer3" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())
        justRun { heartbeatService.recordCommit(any()) }

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertTrue(result)
    }

    @Test
    fun `should use the same idempotency key for replication and commit recording`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081")
        val capturedKey = slot<String>()

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())
        every { heartbeatService.recordCommit(capture(capturedKey)) } returns Unit

        val (sut) = createSut(peers, heartbeatService, httpClient)

        sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(capturedKey.isCaptured) },
            { assertTrue(runCatching { java.util.UUID.fromString(capturedKey.captured) }.isSuccess) },
        )
    }

    @Test
    fun `should not send commit broadcasts when quorum is not reached`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)

        val (sut) = createSut(peers, heartbeatService, httpClient)

        sut.replicate(LockOperation.CREATE, createLock())

        verify(exactly = 0) { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) }
    }

    @Test
    fun `should replicate RENEW operation successfully`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>(relaxed = true)
        val peers = listOf("http://peer1:8081")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.RENEW, createLock())

        assertTrue(result)
    }

    @Test
    fun `should replicate RELEASE operation successfully`() {
        val httpClient = mockk<HttpClient>()
        val heartbeatService = mockk<HeartbeatService>(relaxed = true)
        val peers = listOf("http://peer1:8081")

        every { httpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { httpClient.sendAsync(any(), any<HttpResponse.BodyHandler<Void>>()) } returns CompletableFuture.completedFuture(mockk())

        val (sut) = createSut(peers, heartbeatService, httpClient)

        val result = sut.replicate(LockOperation.RELEASE, createLock())

        assertTrue(result)
    }
}
