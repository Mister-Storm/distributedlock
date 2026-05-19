package org.misterstorm.distributedlock.infra.assync.subscriber

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.infra.assync.publisher.LockEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class QueueSubscriber(
    private val lockRepository: LockRepository,
) {

    @EventListener
    fun handleEvent(event: LockEvent) = lockRepository.addQueue(event.lock)
}