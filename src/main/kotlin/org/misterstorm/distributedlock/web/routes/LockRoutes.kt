package org.misterstorm.distributedlock.web.routes

import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.misterstorm.distributedlock.core.usecases.lock.CreateLockUseCase
import org.misterstorm.distributedlock.core.usecases.lock.GetResourceLockStatusUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockReleaseUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockRenewUseCase
import org.misterstorm.distributedlock.web.routes.responses.ErrorResponse
import org.misterstorm.distributedlock.web.routes.spec.LockRoutesSpec
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/lock")
class LockRoutes(
    private val createLockUseCase: CreateLockUseCase,
    private val lockReleaseUseCase: LockReleaseUseCase,
    private val lockRenewUseCase: LockRenewUseCase,
    private val getResourceLockStatusUseCase: GetResourceLockStatusUseCase,
) : LockRoutesSpec {
    override suspend fun lock(lock: LockCandidate): ResponseEntity<*> =
        createLockUseCase.execute(lock).fold(
            { error ->
                val result = ErrorResponse.from(error)
                ResponseEntity.status(result.httpStatus).body(result)
                     },
             { lock -> ResponseEntity
                .status(HttpStatus.CREATED).body(lock) }
        )


    override suspend fun unlock(lock: LockCandidate): ResponseEntity<*> =
        lockReleaseUseCase.execute(lock).fold(
            { error ->
                val result = ErrorResponse.from(error)
                ResponseEntity.status(result.httpStatus).body(result)
            },
            { ResponseEntity.noContent().build() }
        )

    override suspend fun renew(
        lock: LockCandidate
    ): ResponseEntity<*> =
        lockRenewUseCase.execute(lock).fold(
            { error ->
                val result = ErrorResponse.from(error)
                ResponseEntity.status(result.httpStatus).body(result)
            },
            { lock -> ResponseEntity.ok(lock) }
        )

    override suspend fun getLock(key: String): ResponseEntity<*> =
        getResourceLockStatusUseCase.execute(key).fold(
            { error ->
                val result = ErrorResponse.from(error)
                ResponseEntity.status(result.httpStatus).body(result)
            },
            { lock -> ResponseEntity.ok(lock) }
        )
}