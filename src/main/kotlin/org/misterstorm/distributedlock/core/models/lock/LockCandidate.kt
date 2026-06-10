package org.misterstorm.distributedlock.core.models.lock

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size

data class LockCandidate(
    @field:NotBlank
    @field:Size(max = 256)
    val key: String,
    @field:NotBlank
    @field:Size(max = 256)
    val clientId: String,
)
