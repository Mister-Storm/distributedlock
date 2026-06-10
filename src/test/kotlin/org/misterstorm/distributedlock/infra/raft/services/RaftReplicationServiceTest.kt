package org.misterstorm.distributedlock.infra.raft.services

import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.misterstorm.distributedlock.core.adapter.CommitTracker
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import org.misterstorm.distributedlock.infra.chaos.ClusterHttpClient
import tools.jackson.databind.ObjectMapper
import tools.jackson.module.kotlin.jsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.net.http.HttpRequest
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
        commitTracker: CommitTracker = mockk(relaxed = true),
        clusterHttpClient: ClusterHttpClient = mockk(),
    ): Triple<RaftReplicationService, CommitTracker, ClusterHttpClient> {
        val peerRepository = mockk<PeerRepository>(relaxed = true)
        every { peerRepository.getPeerUrls() } returns peers
        every { peerRepository.getReachablePeerUrls() } returns peers
        return Triple(
            RaftReplicationService(peerRepository, commitTracker, clusterHttpClient, objectMapper),
            commitTracker,
            clusterHttpClient,
        )
    }

    @Test
    fun `should return true immediately when there are no peers`() {
        val (sut, commitTracker, _) = createSut(peers = emptyList())

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 0) { commitTracker.recordCommit(any()) } },
        )
    }

    @Test
    fun `should return true and record commit when all peers acknowledge`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())
        justRun { commitTracker.recordCommit(any()) }

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 1) { commitTracker.recordCommit(any()) } },
            { verify(exactly = peers.size) { clusterHttpClient.sendAsyncDiscarding(any()) } },
        )
    }

    @Test
    fun `should return true with a single peer that acknowledges`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())
        justRun { commitTracker.recordCommit(any()) }

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.RELEASE, createLock())

        assertTrue(result)
    }

    @Test
    fun `should return true when quorum is reached even if not all peers acknowledge`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082", "http://peer3:8083")

        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer1" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer2" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer3" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())
        justRun { commitTracker.recordCommit(any()) }

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(result) },
            { verify(exactly = 1) { commitTracker.recordCommit(any()) } },
        )
    }

    @Test
    fun `should return false when no peer acknowledges and quorum is not reached`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { commitTracker.recordCommit(any()) } },
            { verify(exactly = 0) { clusterHttpClient.sendAsyncDiscarding(any()) } },
        )
    }

    @Test
    fun `should return false when there is only one peer and it does not acknowledge`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(500)

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.RENEW, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { commitTracker.recordCommit(any()) } },
        )
    }

    @Test
    fun `should count peer as nack when HTTP call throws an exception`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } throws java.io.IOException("Connection refused")

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertFalse(result) },
            { verify(exactly = 0) { commitTracker.recordCommit(any()) } },
        )
    }

    @Test
    fun `should still reach quorum when some peers throw and others ack`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082", "http://peer3:8083")

        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer1" }, any<HttpResponse.BodyHandler<String>>()) } throws java.io.IOException("timeout")
        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer2" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.send(match<HttpRequest> { it.uri().host == "peer3" }, any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())
        justRun { commitTracker.recordCommit(any()) }

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.CREATE, createLock())

        assertTrue(result)
    }

    @Test
    fun `should use the same idempotency key for replication and commit recording`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081")
        val capturedKey = slot<String>()

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())
        every { commitTracker.recordCommit(capture(capturedKey)) } returns Unit

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        sut.replicate(LockOperation.CREATE, createLock())

        assertAll(
            { assertTrue(capturedKey.isCaptured) },
            { assertTrue(runCatching { java.util.UUID.fromString(capturedKey.captured) }.isSuccess) },
        )
    }

    @Test
    fun `should not send commit broadcasts when quorum is not reached`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>()
        val peers = listOf("http://peer1:8081", "http://peer2:8082")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(503)

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        sut.replicate(LockOperation.CREATE, createLock())

        verify(exactly = 0) { clusterHttpClient.sendAsyncDiscarding(any()) }
    }

    @Test
    fun `should replicate RENEW operation successfully`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>(relaxed = true)
        val peers = listOf("http://peer1:8081")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.RENEW, createLock())

        assertTrue(result)
    }

    @Test
    fun `should replicate RELEASE operation successfully`() {
        val clusterHttpClient = mockk<ClusterHttpClient>()
        val commitTracker = mockk<CommitTracker>(relaxed = true)
        val peers = listOf("http://peer1:8081")

        every { clusterHttpClient.send(any(), any<HttpResponse.BodyHandler<String>>()) } returns mockHttpResponse(200)
        every { clusterHttpClient.sendAsyncDiscarding(any()) } returns CompletableFuture.completedFuture(mockk())

        val (sut) = createSut(peers, commitTracker, clusterHttpClient)

        val result = sut.replicate(LockOperation.RELEASE, createLock())

        assertTrue(result)
    }
}
