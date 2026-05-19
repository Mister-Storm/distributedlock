package org.misterstorm.distributedlock.core.usecases

import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class AbstractUseCase<T, U> {
    val log : Logger = LoggerFactory.getLogger(javaClass)
    abstract suspend fun execute(input: T): U
}