package org.misterstorm.distributedlock.infra.raft.services

import kotlinx.coroutines.runBlocking
import org.misterstorm.distributedlock.core.usecases.raft.StartElectionUseCase
import org.misterstorm.distributedlock.infra.raft.repository.NodeStateRepositoryInMemory
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

@Service
class ElectionService(
    private val nodeStateRepository: NodeStateRepositoryInMemory,
    private val startElectionUseCase: StartElectionUseCase,
    @Value("\${distributedlock.node.electionTimeout:6000}") private val electionTimeout: Long,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRateString = "\${distributedlock.node.electionTimeout:6000}")
    fun checkElectionTimeout() {
        if (nodeStateRepository.isLeader() || !nodeStateRepository.isHeartbeatExpired(electionTimeout)) return
        startElection()
    }

    fun startElection() = runBlocking { startElectionUseCase.execute(Unit) }
}