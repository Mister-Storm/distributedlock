package org.misterstorm.distributedlock.core.errors

sealed interface BusinessError {
    data class LockAlreadyExists(val key: String): BusinessError
    class ApplicantReleaseIsNotOwner: BusinessError
    class LockNotFound: BusinessError
    class UnexpectedException: BusinessError
}