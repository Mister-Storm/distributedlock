package org.misterstorm.distributedlock.infra.assync.publisher

import org.misterstorm.distributedlock.core.async.Publisher
import org.misterstorm.distributedlock.core.models.lock.Lock
import org.springframework.context.ApplicationEvent
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component

@Component
class QueuePublisher(
    private val eventPublisher: ApplicationEventPublisher
): Publisher<Lock> {
    override fun publish(value: Lock) =  eventPublisher.publishEvent(LockEvent(this, value))
}

class LockEvent(source: Any, val lock: Lock): ApplicationEvent(source)