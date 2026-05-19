package org.misterstorm.distributedlock.core.async

fun interface Publisher<T> {
    fun publish(value: T)
}