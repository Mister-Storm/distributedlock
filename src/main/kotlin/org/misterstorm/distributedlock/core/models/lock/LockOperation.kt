package org.misterstorm.distributedlock.core.models.lock

enum class LockOperation {
    CREATE,
    RELEASE,
    RENEW,
    ENQUEUE,
    /** Remove o primeiro item da fila para a key e cria o lock (promoção atômica nos followers). */
    PROMOTE,
}