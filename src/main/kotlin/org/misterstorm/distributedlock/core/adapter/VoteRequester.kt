package org.misterstorm.distributedlock.core.adapter

import org.misterstorm.distributedlock.core.models.raft.VoteInput
import org.misterstorm.distributedlock.core.models.raft.VoteOutput

interface VoteRequester {
    fun requestVote(peerUrl: String, input: VoteInput): VoteOutput?
}

