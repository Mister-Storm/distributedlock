package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.repository.NodeStateRepositoryInMemory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.LocalDateTime

@Service
class ExpiredLockCleanupService(
    private val lockRepository: LockRepository,
    private val nodeState: NodeStateRepositoryInMemory,
    private val raftReplicationService: RaftReplicationService,
    @Value("\${distributedlock.expirationTime:120}") private val expirationTime: Long,
) {
    private val log: Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "#{'\${distributedlock.lockCleanupInterval:3000}'}")
    fun cleanupExpiredLocks() {
        if (!nodeState.isLeader()) return

        lockRepository.getAllLocks()
            .filter { it.isExpired() }
            .forEach { expiredLock ->
                MDC.put("lockKey", expiredLock.key)
                MDC.put("lockOwner", expiredLock.lockOwner)
                log.info("Cleaning up expired lock")

                lockRepository.release(expiredLock)
                val released = raftReplicationService.replicate(LockOperation.RELEASE, expiredLock)
                if (!released) {
                    log.warn("Quorum not reached while releasing expired lock, will retry on next cycle")
                    lockRepository.create(expiredLock)
                    MDC.remove("lockKey"); MDC.remove("lockOwner")
                    return@forEach
                }

                promoteFromQueue(expiredLock.key)

                MDC.remove("lockKey"); MDC.remove("lockOwner")
            }
    }

    private fun promoteFromQueue(key: String) {
        if (!lockRepository.hasKeyInQueue(key)) return
        val candidate = lockRepository.dequeue(key)
        val promoted = candidate.copy(expirationTime = LocalDateTime.now().plusSeconds(expirationTime))
        lockRepository.create(promoted)
        MDC.put("promotedOwner", promoted.lockOwner)
        log.info("Promoting queued lock after expiry")
        val created = raftReplicationService.replicate(LockOperation.CREATE, promoted)
        if (!created) {
            log.warn("Quorum not reached while promoting queued lock, re-enqueuing")
            lockRepository.release(promoted)
            lockRepository.addQueue(candidate)
        }
        MDC.remove("promotedOwner")
    }
}
