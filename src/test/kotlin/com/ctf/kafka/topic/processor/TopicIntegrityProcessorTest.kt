package com.ctf.kafka.topic.processor

import com.ctf.kafka.topic.extentions.addCoordinateToInputMap
import com.ctf.kafka.topic.processor.context.Failure
import com.ctf.kafka.topic.strategy.KafkaCoordinate
import com.ctf.kafka.topic.strategy.producerRecordStrategyBuilder
import com.ctf.kafka.topic.strategy.topicContextExpectations
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.context.ActiveProfiles
import java.util.*

private const val TOPIC_NAME = "test-topic"

@SpringBootTest(properties = ["runner.enabled=false"])
@ActiveProfiles("Test")
@EmbeddedKafka(topics = [TOPIC_NAME], partitions = 1)
@ExtendWith(OutputCaptureExtension::class)
class TopicIntegrityProcessorTest {

    @Autowired
    private lateinit var template: KafkaTemplate<String, ByteArray>

    @Autowired
    private lateinit var unitUnderTest: PartitionScopedKeyProcessor

    @Test
    fun `Verify correct context produced for offset which is latest for key`() = runBlocking {
        val key = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key)
            randomRecords(TOPIC_NAME, 5)
        }.execute(template)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(strategy.firstForKey(key))
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            latestForKey(key, strategy::firstForKey)
        }.verify(result)
    }

    @Test
    fun `Verify for single offset which is not latest for key`() = runBlocking {
        val key = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            recordWithKnownKey(TOPIC_NAME, key)
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key)
        }.execute(template)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(strategy.firstForKey(key))
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            notLatestForKey(key, strategy::firstForKey, strategy::latestForKey)
        }.verify(result)
    }

    @Test
    fun `Verify for multiple offsets for same key where one is latest`() = runBlocking {
        val key = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            recordWithKnownKey(TOPIC_NAME, key)
            randomRecords(TOPIC_NAME, 5)
            recordWithKnownKey(TOPIC_NAME, key)
        }.execute(template)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(strategy.firstForKey(key))
                it.addCoordinateToInputMap(strategy.latestForKey(key))
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            notLatestForKey(key, strategy::firstForKey, strategy::latestForKey)
            latestForKey(key, strategy::latestForKey)
        }.verify(result)
    }

    @Test
    fun `Verify context is correct when topic does not exist`() = runBlocking {
        val coordinateForMissingTopic = KafkaCoordinate("MISSING_TOPIC", 0, 10, 0)
        val result = unitUnderTest.processTopic("MISSING_TOPIC",
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(coordinateForMissingTopic)
            })

        // Expectations
        topicContextExpectations("MISSING_TOPIC") {
            failureForCoordinate(coordinateForMissingTopic, Failure.TOPIC_NOT_EXISTS)
        }.verify(result)
    }

    @Test
    fun `Verify if offset is out of range then context is updated accordingly`() = runBlocking {
        val outOfRangeCoordinate = KafkaCoordinate(TOPIC_NAME, 0, 10_000, 0)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(outOfRangeCoordinate)
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            failureForCoordinate(outOfRangeCoordinate, Failure.OFFSET_NOT_WITHIN_RANGE)
        }.verify(result)
    }

    @Test
    fun `Verify if partition does not exist then context is updated accordingly`() = runBlocking {
        val missingPartitionCoordinate = KafkaCoordinate(TOPIC_NAME, 1, 2, 0)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(missingPartitionCoordinate)
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            failureForCoordinate(missingPartitionCoordinate, Failure.PARTITION_NOT_EXISTS)
        }.verify(result)
    }

    @Test
    fun `Verify that offset of interest with missing key updates context accordingly`() = runBlocking {
        val referenceToOffsetWithNoKey = UUID.randomUUID().toString()

        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            recordWithNoKey(TOPIC_NAME, referenceToOffsetWithNoKey)
        }.execute(template)

        val coordinateWithMissingKey = strategy.coordinateWithMissingKey(referenceToOffsetWithNoKey)
        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(coordinateWithMissingKey)
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            failureForCoordinate(coordinateWithMissingKey, Failure.OFFSET_MISSING_KEY)
        }.verify(result)
    }

    @Test
    fun `Verify that processing can complete early if no offsets of interest have keys`(
        output: CapturedOutput
    ): Unit = runBlocking {
        val referenceToOffsetWithNoKey1 = UUID.randomUUID().toString()
        val referenceToOffsetWithNoKey2 = UUID.randomUUID().toString()


        // Producer Strategy
        val strategy = producerRecordStrategyBuilder {
            recordWithNoKey(TOPIC_NAME, referenceToOffsetWithNoKey1)
            randomRecords(TOPIC_NAME, 5)
            recordWithNoKey(TOPIC_NAME, referenceToOffsetWithNoKey2)
            randomRecords(TOPIC_NAME, 50)
        }.execute(template)

        val coordinateWithMissingKey1 = strategy.coordinateWithMissingKey(referenceToOffsetWithNoKey1)
        val coordinateWithMissingKey2 = strategy.coordinateWithMissingKey(referenceToOffsetWithNoKey2)

        val result = unitUnderTest.processTopic(TOPIC_NAME,
            mutableMapOf<Int, SortedSet<Long>>().also {
                it.addCoordinateToInputMap(coordinateWithMissingKey1)
                it.addCoordinateToInputMap(coordinateWithMissingKey2)
            })

        // Expectations
        topicContextExpectations(TOPIC_NAME) {
            failureForCoordinate(coordinateWithMissingKey1, Failure.OFFSET_MISSING_KEY)
            failureForCoordinate(coordinateWithMissingKey2, Failure.OFFSET_MISSING_KEY)
        }.verify(result)

        output.out shouldContain "Completing partition 0 early as no keys left to check"
    }
}
