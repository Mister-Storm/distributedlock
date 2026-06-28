package org.misterstorm.distributedlock.infra.repository

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.models.lock.ReplicaEntry
import org.misterstorm.distributedlock.core.usecases.lock.support.createLock
import java.time.LocalDateTime

class LockRepositoryInMemoryTest {

    @Test
    fun `PROMOTE commit should remove lock from queue and store it as active lock`() {
        val repo = LockRepositoryInMemory()
        val queued = createLock(key = "resource-x", lockOwner = "client-a")
        repo.addQueue(queued)
        val promoted = queued.copy(expirationTime = LocalDateTime.now().plusSeconds(120))

        repo.savePending(ReplicaEntry("promote-1", LockOperation.PROMOTE, promoted))
        repo.commit("promote-1")

        assertFalse(repo.hasKeyInQueue("resource-x"))
        assertEquals("client-a", repo.getByKey("resource-x")?.lockOwner)
        assertTrue(repo.getAllInQueue().isEmpty())
    }

    @Test
    fun `PROMOTE commit should not duplicate active lock when one already exists`() {
        val repo = LockRepositoryInMemory()
        val existing = createLock(key = "resource-x", lockOwner = "current-owner")
        repo.create(existing)
        val queued = createLock(key = "resource-x", lockOwner = "waiting-client")
        repo.addQueue(queued)
        val promoted = queued.copy(expirationTime = LocalDateTime.now().plusSeconds(120))

        repo.savePending(ReplicaEntry("promote-2", LockOperation.PROMOTE, promoted))
        repo.commit("promote-2")

        assertEquals("current-owner", repo.getByKey("resource-x")?.lockOwner)
        assertFalse(repo.hasKeyInQueue("resource-x"))
    }

    @Test
    fun `ENQUEUE commit should allow multiple waiters for the same key`() {
        val repo = LockRepositoryInMemory()
        val clients = listOf("client-a", "client-b", "client-c")
        clients.forEachIndexed { index, clientId ->
            val lock = createLock(key = "resource-1", lockOwner = clientId)
            val key = "enqueue-$index"
            repo.savePending(ReplicaEntry(key, LockOperation.ENQUEUE, lock))
            repo.commit(key)
        }

        assertEquals(3, repo.getAllInQueue().size)
        assertTrue(repo.hasClientInQueue("resource-1", "client-a"))
        assertTrue(repo.hasClientInQueue("resource-1", "client-b"))
        assertTrue(repo.hasClientInQueue("resource-1", "client-c"))
    }

    @Test
    fun `ENQUEUE commit should be idempotent for the same client`() {
        val repo = LockRepositoryInMemory()
        val lock = createLock(key = "resource-1", lockOwner = "client-a")

        repo.savePending(ReplicaEntry("enqueue-1", LockOperation.ENQUEUE, lock))
        repo.commit("enqueue-1")
        repo.savePending(ReplicaEntry("enqueue-2", LockOperation.ENQUEUE, lock))
        repo.commit("enqueue-2")

        assertEquals(1, repo.getAllInQueue().size)
    }

    @Test
    fun `PROMOTE commit should remove only the promoted waiter when multiple wait on same key`() {
        val repo = LockRepositoryInMemory()
        val waiting = listOf(
            createLock(key = "resource-1", lockOwner = "client-a"),
            createLock(key = "resource-1", lockOwner = "client-b"),
            createLock(key = "resource-1", lockOwner = "client-c"),
        )
        waiting.forEach { repo.addQueue(it) }

        val promoted = waiting[0].copy(expirationTime = LocalDateTime.now().plusSeconds(120))
        repo.savePending(ReplicaEntry("promote-multi", LockOperation.PROMOTE, promoted))
        repo.commit("promote-multi")

        assertEquals("client-a", repo.getByKey("resource-1")?.lockOwner)
        assertEquals(2, repo.getAllInQueue().size)
        assertTrue(repo.hasClientInQueue("resource-1", "client-b"))
        assertTrue(repo.hasClientInQueue("resource-1", "client-c"))
        assertFalse(repo.hasClientInQueue("resource-1", "client-a"))
    }

    @Test
    fun `loadSnapshot should load all waiters for the same key`() {
        val repo = LockRepositoryInMemory()
        val queued = listOf(
            createLock(key = "resource-1", lockOwner = "client-a"),
            createLock(key = "resource-1", lockOwner = "client-b"),
            createLock(key = "resource-1", lockOwner = "client-c"),
        )

        repo.loadSnapshot(emptyList(), queued)

        assertEquals(3, repo.getAllInQueue().size)
        assertTrue(repo.hasClientInQueue("resource-1", "client-a"))
        assertTrue(repo.hasClientInQueue("resource-1", "client-b"))
        assertTrue(repo.hasClientInQueue("resource-1", "client-c"))
    }
}
