package org.misterstorm.distributedlock.core.usecases.lock.support

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.infra.raft.models.NodeState
import java.time.LocalDateTime

private const val TEST_KEY = "test_key"

private const val TEST_CLIENT_ID = "test_client_id"

fun createLockCandidate(
    key: String = TEST_KEY,
    clientId: String = TEST_CLIENT_ID,
): LockCandidate = LockCandidate(
    key = key,
    clientId = clientId,
)

fun createLock(
    key: String = TEST_KEY,
    lockOwner: String = TEST_CLIENT_ID,
    expirationTime: LocalDateTime = LocalDateTime.now().plusSeconds(120),

    ): Lock = Lock(
    key = key,
    lockOwner = lockOwner,
    expirationTime = expirationTime,
)

fun createNodeState(
    nomeName: String = "node1",
    nodeUrl: String = "http://localhost:8080",
    electionTime: Long = 1000L,
): NodeState {
    val nodeState = NodeState(
        nodeName = nomeName,
        nodeUrl = nodeUrl,
        electionTimeout = electionTime,
    )
    nodeState.becomeLeader()
    return nodeState
}