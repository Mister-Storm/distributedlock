package org.misterstorm.distributedlock.web.routes.responses

import org.misterstorm.distributedlock.core.errors.BusinessError
import org.springframework.http.HttpStatus

data class ErrorResponse(
    val errorMessage: String,
    val httpStatus: HttpStatus,
    val timestamp: Long
) {
    companion object {
        fun from(error: BusinessError): ErrorResponse = when (error) {
            is BusinessError.LockAlreadyExists -> ErrorResponse(
                errorMessage = "Lock with key ${error.key} already exists",
                httpStatus = HttpStatus.BAD_REQUEST,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.UnexpectedException -> ErrorResponse(
                errorMessage = "An unexpected error occurred",
                httpStatus = HttpStatus.INTERNAL_SERVER_ERROR,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.LockNotFound -> ErrorResponse(
                errorMessage = "Not found",
                httpStatus = HttpStatus.NOT_FOUND,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.ApplicantReleaseIsNotOwner -> ErrorResponse(
                errorMessage = "Not applicable. The applicant isn't owner of this lock",
                httpStatus = HttpStatus.CONFLICT,
                timestamp = System.currentTimeMillis()
            )
        }
    }
}
