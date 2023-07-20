package com.ctf.kafka.topic.integation

import com.ctf.kafka.topic.config.properties.PoolingProperties
import com.ctf.kafka.topic.processor.PartitionScopedKeyProcessor
import com.ctf.kafka.topic.reporting.ReportingDelegator
import com.ctf.kafka.topic.runner.Runner
import com.ctf.kafka.topic.strategy.KafkaCoordinate
import com.ctf.kafka.topic.strategy.ProducerRecordStrategy
import com.ctf.kafka.topic.strategy.producerRecordStrategyBuilder
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import mu.KotlinLogging
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import kotlin.system.measureTimeMillis

private const val INPUT_FILE_DIR = "build/testing/input"
private const val STRESS_TOPIC_1 = "stress-test-topic-1"

private const val SUPERSEDED_OUTPUT_FILE = "build/testing/output/superseded-validated-offsets"

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(properties = ["runner.enabled=false"])
@ActiveProfiles("Test")
@EmbeddedKafka(topics = [STRESS_TOPIC_1], partitions = 12)
@Tag("StressTest")
class StressTest {

    private val logger = KotlinLogging.logger {}

    @Autowired
    private lateinit var template: KafkaTemplate<String, ByteArray>

    @Autowired
    private lateinit var topicIntegrityProcessor: PartitionScopedKeyProcessor

    @Autowired
    private lateinit var poolingProperties: PoolingProperties

    @Autowired
    private lateinit var reporting: ReportingDelegator

    @Test
    fun `Stress Test`() = runTest {
        val markerKey1 = UUID.randomUUID().toString()
        val markerKey2 = UUID.randomUUID().toString()

        val strategy = producerRecordStrategyBuilder {
            randomRecords(STRESS_TOPIC_1, 1_000_000)
            recordWithKnownKey(STRESS_TOPIC_1, markerKey1)
            recordWithKnownKey(STRESS_TOPIC_1, markerKey2)
            randomRecords(STRESS_TOPIC_1, 100_000, true)
            // Adding more records the processor needs to traverse
            randomRecords(STRESS_TOPIC_1, 15_000_000)
            randomRecords(STRESS_TOPIC_1, 20_000, true)
            randomRecords(STRESS_TOPIC_1, 5_000_000)
            // This last one should mean that the earliest offset for this key is not included in the output
            // and neither will the offset for this key as it won't be included in the input
            // This is essentially proving the process is consuming till the latest identified offset
            recordWithKnownKey(STRESS_TOPIC_1, markerKey1)
        }

        measureTimeMillis {
            strategy.execute(template)
        }.also {
            logger.info { "Sending records took ${Duration.of(it, ChronoUnit.MILLIS).toSeconds()} seconds" }
        }

        val inputFilePath = strategy.createInputFileFromEarlierOffsetsForKnownKeys()

        // Run
        measureTimeMillis {
            Runner(topicIntegrityProcessor, poolingProperties, reporting)
                .run(arrayOf(inputFilePath))
        }.also {
            logger.info { "Processing took ${Duration.of(it, ChronoUnit.MILLIS).toSeconds()} seconds" }
        }

        verifyOutputFileContainsCoords(listOf(strategy.firstForKey(markerKey1)))
    }

    fun verifyOutputFileContainsCoords(coordinates: List<KafkaCoordinate>) {
        val coordinateStrings = coordinates.map(KafkaCoordinate::toString)
        File(SUPERSEDED_OUTPUT_FILE).readLines().toList() shouldContainExactlyInAnyOrder coordinateStrings
    }

    private fun ProducerRecordStrategy.createInputFileFromEarlierOffsetsForKnownKeys() =
        createInputFileAndReturnPath(this.trackedCoordinatesForKnownKeys.values.map { it.first() })

    fun createInputFileAndReturnPath(coordinates: List<KafkaCoordinate>): String {
        val inputPath = Paths.get(INPUT_FILE_DIR, UUID.randomUUID().toString())
        Files.createDirectories(inputPath.parent)
        inputPath.toFile().printWriter().use { out ->
            coordinates.forEach {
                out.appendLine(it.toString())
            }
        }
        return inputPath.toString()
    }
}
