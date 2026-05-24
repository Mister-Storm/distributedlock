package org.misterstorm.distributedlock.web.routes

import org.misterstorm.distributedlock.core.errors.BusinessError
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.CreateLockUseCase
import org.misterstorm.distributedlock.core.usecases.lock.GetResourceLockStatusUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockReleaseUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockRenewUseCase
import org.misterstorm.distributedlock.web.routes.responses.ErrorResponse
import org.misterstorm.distributedlock.web.routes.spec.LockRoutesSpec
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
class LockRoutes(
    private val createLockUseCase: CreateLockUseCase,
    private val lockReleaseUseCase: LockReleaseUseCase,
    private val lockRenewUseCase: LockRenewUseCase,
    private val getResourceLockStatusUseCase: GetResourceLockStatusUseCase,
) : LockRoutesSpec {

    private val log = LoggerFactory.getLogger(javaClass)

    override suspend fun lock(lock: LockCandidate): ResponseEntity<*> {
        initMdc("POST /lock", lock.clientId, lock.key)
        log.info("Lock request received")
        return createLockUseCase.execute(lock).fold(
            { error -> handleError(error, "/lock") },
            { result -> ResponseEntity.status(HttpStatus.CREATED).body(result) }
        ).also { MDC.clear() }
    }

    override suspend fun unlock(lock: LockCandidate): ResponseEntity<*> {
        initMdc("DELETE /lock", lock.clientId, lock.key)
        log.info("Unlock request received")
        return lockReleaseUseCase.execute(lock).fold(
            { error -> handleError(error, "/lock") },
            { ResponseEntity.noContent().build<Unit>() }
        ).also { MDC.clear() }
    }

    override suspend fun renew(lock: LockCandidate): ResponseEntity<*> {
        initMdc("PUT /lock/renew", lock.clientId, lock.key)
        log.info("Lock renew request received")
        return lockRenewUseCase.execute(lock).fold(
            { error -> handleError(error, "/lock") },
            { result -> ResponseEntity.ok(result) }
        ).also { MDC.clear() }
    }

    override suspend fun getLock(key: String): ResponseEntity<*> {
        MDC.put("traceId", UUID.randomUUID().toString())
        MDC.put("lockKey", key)
        log.info("Get lock status request received")
        return getResourceLockStatusUseCase.execute(key).fold(
            { error ->
                val result = ErrorResponse.from(error)
                ResponseEntity.status(result.httpStatus).body(result)
            },
            { result -> ResponseEntity.ok(result) }
        ).also { MDC.clear() }
    }

    private fun handleError(error: BusinessError, path: String): ResponseEntity<*> {
        if (error is BusinessError.NotLeader && error.leaderUrl != null) {
            MDC.put("redirectTo", "${error.leaderUrl}$path")
            log.info("Redirecting request to leader")
            MDC.remove("redirectTo")
            return ResponseEntity
                .status(HttpStatus.TEMPORARY_REDIRECT)
                .header("Location", "${error.leaderUrl}$path")
                .build<Unit>()
        }
        val result = ErrorResponse.from(error)
        return ResponseEntity.status(result.httpStatus).body(result)
    }

    private fun initMdc(operation: String, clientId: String, lockKey: String) {
        MDC.put("traceId", UUID.randomUUID().toString())
        MDC.put("operation", operation)
        MDC.put("clientId", clientId)
        MDC.put("lockKey", lockKey)
    }
}