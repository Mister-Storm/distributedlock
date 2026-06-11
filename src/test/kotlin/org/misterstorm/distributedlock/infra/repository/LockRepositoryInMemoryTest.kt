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
}
