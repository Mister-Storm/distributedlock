package org.misterstorm.distributedlock.infra.raft

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "distributedlock.raft")
class RaftProperties {
    val seeds: List<String> = mutableListOf()
}