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
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*

private const val INPUT_FILE_DIR = "build/testing/input"
private const val OUTPUT_FILE = "build/testing/output/latest-validated-offsets"
private const val TOPIC_NAME = "integration-test-topic"

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest(properties = ["runner.enabled=false"])
@ActiveProfiles("Test")
@EmbeddedKafka(topics = [TOPIC_NAME], partitions = 2)
class VerifiedCoordinateIntegrationTest {

    @Autowired
    private lateinit var template: KafkaTemplate<String, ByteArray>

    @Autowired
    private lateinit var topicIntegrityProcessor: PartitionScopedKeyProcessor

    @Autowired
    private lateinit var poolingProperties: PoolingProperties

    @Autowired
    private lateinit var reporting: ReportingDelegator

    @Test
    fun `Successfully process and produce reports for single topic`() = runTest {
        val key1 = UUID.randomUUID().toString()
        val key2 = UUID.randomUUID().toString()
        val key3 = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key1)
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key2)
            recordWithKnownKey(TOPIC_NAME, key3)
        }.execute(template)

        val inputFilePath = strategy.createInputFileFromEarlierOffsetsForKnownKeys()

        // Run
        Runner(topicIntegrityProcessor, poolingProperties, reporting)
            .run(arrayOf(inputFilePath))

        // Verify
        verifyOutputFileContainsCoords(
            listOf(
                strategy.firstForKey(key1),
                strategy.firstForKey(key2),
                strategy.firstForKey(key3)
            )
        )
    }

    @Test
    fun `Successfully process and produce report when not all Inputs are latest`() = runTest {
        val key1 = UUID.randomUUID().toString()
        val key2 = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key1)
            recordWithKnownKey(TOPIC_NAME, key2)
            recordWithKnownKey(TOPIC_NAME, key1)
            randomRecords(TOPIC_NAME, 5)
        }.execute(template)

        val inputFilePath = createInputFileAndReturnPath(
            listOf(
                strategy.firstForKey(key1),
                strategy.latestForKey(key2)
            )
        )

        println("CHRIS - " + strategy.trackedCoordinatesForKnownKeys)

        // Run
        Runner(topicIntegrityProcessor, poolingProperties, reporting)
            .run(arrayOf(inputFilePath))

        // Verify
        verifyOutputFileContainsCoords(
            listOf(
                strategy.firstForKey(key2)
            )
        )
    }

    private fun ProducerRecordStrategy.createInputFileFromEarlierOffsetsForKnownKeys() =
        createInputFileAndReturnPath(this.trackedCoordinatesForKnownKeys.values.map { it.first() })

    fun verifyOutputFileContainsCoords(coordinates: List<KafkaCoordinate>) {
        val coordinateStrings = coordinates.map(KafkaCoordinate::toString)
        File(OUTPUT_FILE).readLines().toList() shouldContainExactlyInAnyOrder coordinateStrings
    }

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
