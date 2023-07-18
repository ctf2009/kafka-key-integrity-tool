package com.ctf.kafka.topic.pooling

import com.ctf.kafka.topic.extensions.valueIfTrueElseNull
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeout
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.seconds

fun interface PoolingSupplier<T> {
    fun create(): T
}

class ResourcePool<T>(
    private val poolName: String = "Unnamed Pool",
    private val supplier: PoolingSupplier<T>,
    val maxPoolSize: Int
) {
    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "[${poolName}] Creating Resource Pool. Max pool size is $maxPoolSize" }
    }

    private val createdResourceCount = AtomicInteger()
    private val acquiredResourceCount = AtomicInteger()

    private val pool: Channel<T> = Channel(maxPoolSize)
    private val mutex: Mutex = Mutex()

    val numResourcesCreated get() = createdResourceCount.get()
    val availableCapacity get() = maxPoolSize - acquiredResourceCount.get()

    suspend fun acquireResource(maxSecondsToWait: Long = 300): T {
        logger.debug { "[${poolName}] Attempting to Acquire Resource" }
        val resource = tryPool() ?: tryCreateResource() ?: tryWaitWithTimeout(maxSecondsToWait)
        acquiredResourceCount.getAndIncrement()
        return resource
    }

    private fun tryPool(): T? {
        val existing = pool.tryReceive()
        return existing.isSuccess.valueIfTrueElseNull { existing.getOrThrow() }
    }

    private suspend fun tryCreateResource(): T? {
        mutex.lock()
        try {
            return canCreateResource().valueIfTrueElseNull { createResource() }
        } finally {
            mutex.unlock()
        }
    }

    private suspend fun tryWaitWithTimeout(maxSecondsToWait: Long): T {
        return withTimeout(timeout = maxSecondsToWait.seconds) {
            pool.receive()
        }
    }

    fun releaseResource(resource: T) {
        logger.debug { "[${poolName}] Releasing Resource" }
        val result = pool.trySend(resource)
        result.exceptionOrNull()
        acquiredResourceCount.decrementAndGet()
    }

    private fun canCreateResource() = createdResourceCount.get() < maxPoolSize

    private fun createResource(): T {
        val resource = supplier.create()
        createdResourceCount.incrementAndGet()
        return resource
    }
}
