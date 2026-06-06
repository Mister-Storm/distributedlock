package org.misterstorm.distributedlock.core.usecases.raft

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.misterstorm.distributedlock.core.adapter.NodeReachabilityChecker
import org.misterstorm.distributedlock.core.models.raft.ExcludeVoteInput
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ProcessExcludeVoteUseCaseTest {

    @Test
    fun `should return exclude true when node is not reachable`() = runTest {
        val checker = mockk<NodeReachabilityChecker>()
        every { checker.isReachable("http://suspect:8080") } returns false
        val sut = ProcessExcludeVoteUseCase(checker)
        val result = sut.execute(ExcludeVoteInput("http://suspect:8080"))
        assertTrue(result.exclude)
    }

    @Test
    fun `should return exclude false when node is reachable`() = runTest {
        val checker = mockk<NodeReachabilityChecker>()
        every { checker.isReachable("http://suspect:8080") } returns true
        val sut = ProcessExcludeVoteUseCase(checker)
        val result = sut.execute(ExcludeVoteInput("http://suspect:8080"))
        assertFalse(result.exclude)
    }
}

