package org.misterstorm.distributedlock.web.routes.responses

import org.misterstorm.distributedlock.core.errors.BusinessError
import org.springframework.http.HttpStatus

data class ErrorResponse(
    val message: String,
    val httpStatus: HttpStatus,
    val timestamp: Long
) {
    companion object {
        fun from(error: BusinessError): ErrorResponse = when (error) {
            is BusinessError.LockAlreadyExists -> ErrorResponse(
                message = "Lock with key ${error.key} already exists. Request has been put in the queue. Please check the status endpoint",
                httpStatus = HttpStatus.ACCEPTED,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.UnexpectedException -> ErrorResponse(
                message = "An unexpected error occurred",
                httpStatus = HttpStatus.INTERNAL_SERVER_ERROR,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.LockNotFound -> ErrorResponse(
                message = "Not found",
                httpStatus = HttpStatus.NOT_FOUND,
                timestamp = System.currentTimeMillis()
            )
            is BusinessError.ApplicantReleaseIsNotOwner -> ErrorResponse(
                message = "Not applicable. The applicant isn't owner of this lock",
                httpStatus = HttpStatus.CONFLICT,
                timestamp = System.currentTimeMillis()
            )
        }
    }
}
