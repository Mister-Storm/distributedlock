package org.misterstorm.distributedlock.web.routes.spec

import org.misterstorm.distributedlock.core.models.lock.Lock
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody

interface LockRoutesSpec {

    @PostMapping
    suspend fun lock(@RequestBody lock: LockCandidate): ResponseEntity<*>

    @DeleteMapping
    suspend fun unlock(@RequestBody lock: LockCandidate) : ResponseEntity<*>

    @PutMapping()
    suspend fun renew(@RequestBody lock: LockCandidate): ResponseEntity<*>

    @GetMapping("/{key}")
    suspend fun getLock(@PathVariable key: String): ResponseEntity<*>
}