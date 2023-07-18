package com.ctf.kafka.topic.pooling

import io.kotest.matchers.equals.shouldBeEqual
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.system.measureTimeMillis

class ResourcePoolTest {

    private val exampleSupplier = PoolingSupplier { Any() }

    @Test
    fun `Verify resources are not eagerly created`() {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 5)
        resourcePool.numResourcesCreated shouldBeEqual 0
        resourcePool.availableCapacity shouldBeEqual 5
    }

    @Test
    fun `Successfully acquire resource`(): Unit = runBlocking {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 2)
        resourcePool.acquireResource()
        resourcePool.numResourcesCreated shouldBeEqual 1
        resourcePool.availableCapacity shouldBeEqual 1
    }

    @Test
    fun `Successfully acquire all resources`(): Unit = runBlocking {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 2)
        resourcePool.acquireResource()
        resourcePool.acquireResource()
        resourcePool.numResourcesCreated shouldBeEqual 2
        resourcePool.availableCapacity shouldBeEqual 0
    }

    @Test
    fun `Exception received when waiting too long for a resource`(): Unit = runBlocking {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 2)
        resourcePool.acquireResource()
        resourcePool.acquireResource()

        resourcePool.availableCapacity shouldBeEqual 0

        assertThrows<TimeoutCancellationException> {
            resourcePool.acquireResource(1)
        }
    }

    @Test
    fun `Resource is successfully returned to the pool`(): Unit = runBlocking {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 1)
        val resource = resourcePool.acquireResource()
        resourcePool.availableCapacity shouldBeEqual 0

        resourcePool.releaseResource(resource)
        resourcePool.availableCapacity shouldBeEqual 1
    }

    @Test
    fun `Coroutine waits for resource to become available`(): Unit = runBlocking {
        val resourcePool = ResourcePool(supplier = exampleSupplier, maxPoolSize = 1)
        val resource = resourcePool.acquireResource()
        resourcePool.availableCapacity shouldBeEqual 0

        val time = measureTimeMillis {
            val deferred = async {
                resourcePool.acquireResource()
            }

            launch {
                delay(1000)
                deferred.isCompleted shouldBe false
                resourcePool.releaseResource(resource)
            }

            deferred.await()
            deferred.isCompleted shouldBe true
        }

        resourcePool.numResourcesCreated shouldBeEqual 1
        resourcePool.availableCapacity shouldBeEqual 0
        time shouldBeGreaterThan 1000
    }
}
