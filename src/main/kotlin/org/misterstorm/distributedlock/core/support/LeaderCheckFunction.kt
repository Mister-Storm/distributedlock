package org.misterstorm.distributedlock.core.support

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.infra.raft.NodeState

fun <T> verifyLeadership(nodeState: NodeState, action: () -> Either<BusinessError, T>,
                         onNotLeader: () -> BusinessError) : Either<BusinessError, T> =
    if(nodeState.isLeader()) {
        action()
    } else {
        onNotLeader().left()
    }

fun <T> verifyQuorum(checkQuorum: () -> Boolean, value: T, vararg fallbacks: (T) -> Any) : Either<BusinessError, T> =
    if(checkQuorum()) {
        value.right()
    } else {
        fallbacks.forEach { fallback -> fallback(value) }
        BusinessError.QuorumNotReached().left()
    }