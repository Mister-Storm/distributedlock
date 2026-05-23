package org.misterstorm.distributedlock.config

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.lock.CreateLockUseCase
import org.misterstorm.distributedlock.core.usecases.lock.GetResourceLockStatusUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockReleaseUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockRenewUseCase
import org.misterstorm.distributedlock.infra.assync.publisher.FailLockPublisher
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import org.misterstorm.distributedlock.infra.raft.services.RaftReplicationService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class UseCaseConfig {

    @Bean
    fun createLockUseCase(
        lockRepository: LockRepository,
        failLockPublisher: FailLockPublisher,
        @Value("\${distributedlock.expirationTime}") expirationTime: Long,
        nodeState: NodeState,
        raftReplicationService: RaftReplicationService,
        ): CreateLockUseCase = CreateLockUseCase(lockRepository, failLockPublisher, expirationTime, nodeState, raftReplicationService)
    @Bean
    fun getResourceLockStatusUseCase(lockRepository: LockRepository): GetResourceLockStatusUseCase =
        GetResourceLockStatusUseCase(lockRepository)
    @Bean
    fun lockReleaseUseCase(
        lockRepository: LockRepository,
        nodeState: NodeState,
        raftReplicationService: RaftReplicationService,
    ): LockReleaseUseCase = LockReleaseUseCase(lockRepository, nodeState, raftReplicationService)
    @Bean
    fun lockRenewUseCase(
        lockRepository: LockRepository,
        nodeState: NodeState,
        raftReplicationService: RaftReplicationService,
    ): LockRenewUseCase = LockRenewUseCase(lockRepository, nodeState, raftReplicationService)
}