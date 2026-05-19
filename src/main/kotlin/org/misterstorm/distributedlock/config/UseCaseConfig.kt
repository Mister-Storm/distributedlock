package org.misterstorm.distributedlock.config

import org.misterstorm.distributedlock.core.repository.LockRepository
import org.misterstorm.distributedlock.core.usecases.lock.CreateLockUseCase
import org.misterstorm.distributedlock.core.usecases.lock.GetResourceLockStatusUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockReleaseUseCase
import org.misterstorm.distributedlock.core.usecases.lock.LockRenewUseCase
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class UseCaseConfig {

    @Bean
    fun createLockUseCase(
        lockRepository: LockRepository,
        @Value("\${distributedlock.expirationTime}") expirationTime: Long,
        ): CreateLockUseCase = CreateLockUseCase(lockRepository, expirationTime)
    @Bean
    fun getResourceLockStatusUseCase(lockRepository: LockRepository): GetResourceLockStatusUseCase =
        GetResourceLockStatusUseCase(lockRepository)
    @Bean
    fun lockReleaseUseCase(
        lockRepository: LockRepository,
    ): LockReleaseUseCase = LockReleaseUseCase(lockRepository)
    @Bean
    fun lockRenewUseCase(
        lockRepository: LockRepository,
    ): LockRenewUseCase = LockRenewUseCase(lockRepository)
}