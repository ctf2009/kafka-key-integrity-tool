package com.ctf.kafka.topic.processor

import com.ctf.kafka.topic.processor.CoordinatesToMapProcessor.processCoordinates
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.system.OutputCaptureExtension

@OptIn(ExperimentalCoroutinesApi::class)
@ExtendWith(OutputCaptureExtension::class)
class CoordinatesToMapProcessorTest {

    @Test
    fun `Verify map is created correctly from incoming`() = runTest {
        val channel = produce {
            send("TOPIC-0-1")
            send("TOPIC2-1-1")
            send("TOPIC2-1-4")
            send("TOPIC-WITH--DASHES-SRC-2-12")
        }

        val result = async {
            processCoordinates(channel, 1)
        }

        result.await() shouldContainExactly mapOf(
            "TOPIC" to mapOf(0 to sortedSetOf(1L)),
            "TOPIC2" to mapOf(1 to sortedSetOf(1L, 4L)),
            "TOPIC-WITH--DASHES-SRC" to mapOf(2 to sortedSetOf(12L))
        )
    }

    @Test
    fun `Invalid Format`() = runTest {
        val job = Job()
        val channel = produce(job) {
            send("TOPIC-0-1-")
        }

        val result = async(job) {
            processCoordinates(channel, 1)
        }

        shouldThrow<IllegalArgumentException> { result.await() }
        job.isActive shouldBe false
    }
}
