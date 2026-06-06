package org.misterstorm.distributedlock.config

import org.misterstorm.distributedlock.core.adapter.NodeReachabilityChecker
import org.misterstorm.distributedlock.core.adapter.NodeStateRepository
import org.misterstorm.distributedlock.core.adapter.PeerRepository
import org.misterstorm.distributedlock.core.adapter.VoteRequester
import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.lock.CreateLockUseCase
import org.misterstorm.distributedlock.core.usecases.lock.GetResourceLockStatusUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockReleaseUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockRenewUseCase
import org.misterstorm.distributedlock.core.usecases.raft.AcceptReplicaUseCase
import org.misterstorm.distributedlock.core.usecases.raft.CommitReplicaUseCase
import org.misterstorm.distributedlock.core.usecases.raft.GetNodeStatusUseCase
import org.misterstorm.distributedlock.core.usecases.raft.GetSnapshotUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessExcludeVoteUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessGossipUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessHeartbeatUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessJoinUseCase
import org.misterstorm.distributedlock.core.usecases.raft.ProcessVoteUseCase
import org.misterstorm.distributedlock.core.usecases.raft.StartElectionUseCase
import org.misterstorm.distributedlock.infra.assync.publisher.FailLockPublisher
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
        nodeStateRepository: NodeStateRepository,
        raftReplicationService: RaftReplicationService,
    ): CreateLockUseCase = CreateLockUseCase(lockRepository, failLockPublisher, expirationTime, nodeStateRepository, raftReplicationService)

    @Bean
    fun getResourceLockStatusUseCase(lockRepository: LockRepository): GetResourceLockStatusUseCase =
        GetResourceLockStatusUseCase(lockRepository)

    @Bean
    fun lockReleaseUseCase(
        lockRepository: LockRepository,
        nodeStateRepository: NodeStateRepository,
        raftReplicationService: RaftReplicationService,
        @Value("\${distributedlock.expirationTime}") expirationTime: Long,
    ): LockReleaseUseCase = LockReleaseUseCase(lockRepository, nodeStateRepository, raftReplicationService, expirationTime)

    @Bean
    fun lockRenewUseCase(
        lockRepository: LockRepository,
        nodeStateRepository: NodeStateRepository,
        raftReplicationService: RaftReplicationService,
    ): LockRenewUseCase = LockRenewUseCase(lockRepository, nodeStateRepository, raftReplicationService)

    @Bean
    fun processHeartbeatUseCase(
        nodeStateRepository: NodeStateRepository,
        lockRepository: LockRepository,
    ): ProcessHeartbeatUseCase = ProcessHeartbeatUseCase(nodeStateRepository, lockRepository)

    @Bean
    fun processVoteUseCase(
        nodeStateRepository: NodeStateRepository,
        peerRepository: PeerRepository,
    ): ProcessVoteUseCase = ProcessVoteUseCase(nodeStateRepository, peerRepository)

    @Bean
    fun startElectionUseCase(
        nodeStateRepository: NodeStateRepository,
        peerRepository: PeerRepository,
        voteRequester: VoteRequester,
    ): StartElectionUseCase = StartElectionUseCase(nodeStateRepository, peerRepository, voteRequester)

    @Bean
    fun processGossipUseCase(peerRepository: PeerRepository): ProcessGossipUseCase =
        ProcessGossipUseCase(peerRepository)

    @Bean
    fun processJoinUseCase(peerRepository: PeerRepository): ProcessJoinUseCase =
        ProcessJoinUseCase(peerRepository)

    @Bean
    fun processExcludeVoteUseCase(reachabilityChecker: NodeReachabilityChecker): ProcessExcludeVoteUseCase =
        ProcessExcludeVoteUseCase(reachabilityChecker)

    @Bean
    fun acceptReplicaUseCase(lockRepository: LockRepository): AcceptReplicaUseCase =
        AcceptReplicaUseCase(lockRepository)

    @Bean
    fun commitReplicaUseCase(lockRepository: LockRepository): CommitReplicaUseCase =
        CommitReplicaUseCase(lockRepository)

    @Bean
    fun getNodeStatusUseCase(
        nodeStateRepository: NodeStateRepository,
        peerRepository: PeerRepository,
        lockRepository: LockRepository,
    ): GetNodeStatusUseCase = GetNodeStatusUseCase(nodeStateRepository, peerRepository, lockRepository)

    @Bean
    fun getSnapshotUseCase(
        nodeStateRepository: NodeStateRepository,
        lockRepository: LockRepository,
    ): GetSnapshotUseCase = GetSnapshotUseCase(nodeStateRepository, lockRepository)
}