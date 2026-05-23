package org.misterstorm.distributedlock.web.routes.spec

import org.misterstorm.distributedlock.infra.raft.services.CommitRequest
import org.misterstorm.distributedlock.infra.raft.services.ExcludeVoteRequest
import org.misterstorm.distributedlock.infra.raft.services.ExcludeVoteResponse
import org.misterstorm.distributedlock.infra.raft.services.GossipMessage
import org.misterstorm.distributedlock.infra.raft.requests.HeartbeatRequest
import org.misterstorm.distributedlock.infra.raft.services.JoinRequest
import org.misterstorm.distributedlock.infra.raft.services.ReplicateRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteRequest
import org.misterstorm.distributedlock.infra.raft.requests.VoteResponse
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping

@RequestMapping("/raft")
interface RaftRoutesSpec {
    @PostMapping("/heartbeat")
    fun heartbeat(@RequestBody request: HeartbeatRequest): ResponseEntity<*>
    @PostMapping("/vote")
    fun vote(@RequestBody request: VoteRequest): ResponseEntity<VoteResponse>
    @PostMapping("/gossip")
    fun gossip(@RequestBody message: GossipMessage): ResponseEntity<GossipMessage>
    @PostMapping("/replicate")
    fun replicate(@RequestBody request: ReplicateRequest): ResponseEntity<*>
    @PostMapping("/commit")
    fun commit(@RequestBody request: CommitRequest): ResponseEntity<*>
    @GetMapping("/status")
    fun status(): ResponseEntity<*>
    @PostMapping("/join")
    fun join(@RequestBody request: JoinRequest): ResponseEntity<GossipMessage>
    @PostMapping("/exclude-vote")
    fun excludeVote(@RequestBody request: ExcludeVoteRequest): ResponseEntity<ExcludeVoteResponse>
}