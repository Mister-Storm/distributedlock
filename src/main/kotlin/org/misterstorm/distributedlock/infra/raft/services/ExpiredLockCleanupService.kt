package org.misterstorm.distributedlock.infra.raft.services

import org.misterstorm.distributedlock.core.models.lock.LockOperation
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class ExpiredLockCleanupService(
    private val lockRepository: LockRepository,
    private val nodeState: NodeState,
    private val raftReplicationService: RaftReplicationService,
) {
    private val log: Logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "#{'\${raft.lockCleanupInterval:3000}'}")
    fun cleanupExpiredLocks() {
        if (!nodeState.isLeader()) return

        lockRepository.getAllLocks()
            .filter { it.isExpired() }
            .forEach { expiredLock ->
                log.info("Cleaning up expired lock [key={}]", expiredLock.key)

                val released = raftReplicationService.replicate(LockOperation.RELEASE, expiredLock)
                if (!released) {
                    log.warn(
                        "Quorum not reached while releasing expired lock [key={}], will retry on next cycle",
                        expiredLock.key
                    )
                    return@forEach
                }

                if (lockRepository.hasKeyInQueue(expiredLock.key)) {
                    val promoted = lockRepository.dequeue(expiredLock.key)
                    log.info(
                        "Promoting queued lock [key={}, owner={}]",
                        promoted.key, promoted.lockOwner
                    )

                    val created = raftReplicationService.replicate(LockOperation.CREATE, promoted)
                    if (!created) {
                        log.warn(
                            "Quorum not reached while promoting queued lock [key={}], re-enqueuing",
                            promoted.key
                        )
                        lockRepository.addQueue(promoted)
                    }
                }
            }
    }
}

