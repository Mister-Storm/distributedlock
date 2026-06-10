package org.misterstorm.distributedlock.web.routes.spec

import jakarta.validation.Valid
import org.misterstorm.distributedlock.core.models.lock.LockCandidate
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping

@RequestMapping("/lock")
interface LockRoutesSpec {

    @PostMapping
    suspend fun lock(@Valid @RequestBody lock: LockCandidate): ResponseEntity<*>

    @DeleteMapping
    suspend fun unlock(@Valid @RequestBody lock: LockCandidate): ResponseEntity<*>

    @PutMapping()
    suspend fun renew(@Valid @RequestBody lock: LockCandidate): ResponseEntity<*>

    @GetMapping("/{key}")
    suspend fun getLock(@PathVariable key: String): ResponseEntity<*>
}