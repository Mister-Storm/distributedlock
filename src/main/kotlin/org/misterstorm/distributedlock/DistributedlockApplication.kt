package org.misterstorm.distributedlock

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import kotlin.jvm.javaClass

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
class DistributedlockApplication

fun main(args: Array<String>) {
    val log : Logger = LoggerFactory.getLogger(DistributedlockApplication::class.java)
    log.info("Starting Distributed Lock Service")
    MDC.put("application", "DistributedLockService")
    MDC.put("value", "value")
    runApplication<DistributedlockApplication>(*args)
}
