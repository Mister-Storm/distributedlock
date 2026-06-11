package org.misterstorm.distributedlock.infra.raft.repository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.raft.PeerReachability
import org.misterstorm.distributedlock.infra.raft.models.RaftProperties

class PeerRepositoryInMemoryTest {

    private fun buildRepo(
        selfName: String = "node1",
        selfUrl: String = "http://localhost:8080",
        seeds: List<String> = emptyList(),
    ): PeerRepositoryInMemory {
        val props = RaftProperties()
        @Suppress("UNCHECKED_CAST")
        (props.seeds as MutableList<String>).addAll(seeds)
        return PeerRepositoryInMemory(selfName, selfUrl, props)
    }

    @Test
    fun `should start with no peers when seeds list is empty`() {
        val repo = buildRepo()
        assertTrue(repo.getRegisteredPeerUrls().isEmpty())
    }

    @Test
    fun `should seed peers excluding self`() {
        val repo = buildRepo(
            selfUrl = "http://localhost:8080",
            seeds = listOf("http://localhost:8080", "http://node2:8081", "http://node3:8082"),
        )
        assertFalse(repo.getRegisteredPeerUrls().contains("http://localhost:8080"))
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node3:8082"))
    }

    @Test
    fun `should include self in getAllNodes`() {
        val repo = buildRepo(selfName = "node1", selfUrl = "http://localhost:8080")
        assertEquals("http://localhost:8080", repo.getAllNodes()["node1"])
    }

    @Test
    fun `should merge new peers`() {
        val repo = buildRepo()
        repo.merge(mapOf("node2" to "http://node2:8081"))
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
    }

    @Test
    fun `should not merge self`() {
        val repo = buildRepo(selfName = "node1", selfUrl = "http://localhost:8080")
        repo.merge(mapOf("node1" to "http://localhost:8080"))
        assertFalse(repo.getRegisteredPeerUrls().contains("http://localhost:8080"))
    }

    @Test
    fun `should keep peer in list when marked unreachable`() {
        val repo = buildRepo(seeds = listOf("http://node2:8081"))
        repo.merge(mapOf("node2" to "http://node2:8081"))
        repeat(PeerRepositoryInMemory.MAX_FAILURES_BEFORE_UNHEALTHY) {
            repo.markUnhealthy("http://node2:8081")
        }
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
        assertTrue(repo.getHealthyPeerUrls().isEmpty())
        assertEquals(PeerReachability.UNHEALTHY, repo.getPeerEntries().first().reachability)
    }

    @Test
    fun `should mark peer reachable again after recovery`() {
        val repo = buildRepo(seeds = listOf("http://node2:8081"))
        repo.merge(mapOf("node2" to "http://node2:8081"))
        repeat(PeerRepositoryInMemory.MAX_FAILURES_BEFORE_UNHEALTHY) {
            repo.markUnhealthy("http://node2:8081")
        }
        repo.markHealthy("http://node2:8081")
        assertTrue(repo.getHealthyPeerUrls().contains("http://node2:8081"))
        assertEquals(0, repo.getPeerEntries().first().consecutiveFailures)
    }

    @Test
    fun `should remove peer only via explicit remove`() {
        val repo = buildRepo(seeds = listOf("http://node2:8081"))
        repo.merge(mapOf("node2" to "http://node2:8081"))
        repo.remove("http://node2:8081")
        assertFalse(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
    }

    @Test
    fun `should replace seed entry when named node merges`() {
        val repo = buildRepo(
            selfUrl = "http://localhost:8080",
            seeds = listOf("http://node2:8081"),
        )
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
        repo.merge(mapOf("node2" to "http://node2:8081"))
        assertTrue(repo.getRegisteredPeerUrls().contains("http://node2:8081"))
        assertEquals(1, repo.getRegisteredPeerUrls().size)
    }
}
