package com.ctf.kafka.topic.strategy

import com.ctf.kafka.topic.processor.context.Failure
import com.ctf.kafka.topic.processor.context.TopicContext
import com.ctf.kafka.topic.processor.context.VerifiedCoordinate
import io.kotest.matchers.collections.shouldContainExactly
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import kotlin.reflect.KFunction1

class TopicContextExpectations(
    private val topicName: String,
) {

    private val expected: MutableList<VerifiedCoordinate> = mutableListOf()

    fun notLatestForKey(
        key: String,
        coordinateSupplier: KFunction1<String, KafkaCoordinate>,
        latestCoordinateSupplier: KFunction1<String, KafkaCoordinate>
    ) {
        val coordinate = coordinateSupplier(key)
        val latestCoordinate = latestCoordinateSupplier(key)
        val topicPartition = TopicPartition(topicName, coordinate.partition)
        expected.add(
            VerifiedCoordinate(
                topic = topicName,
                partition = topicPartition.partition(),
                offsetAndTimestamp = OffsetAndTimestamp(coordinate.offset, coordinate.timestamp),
                identifiedKey = key,
                isLatest = false,
                otherIdentifiedLatest = OffsetAndTimestamp(latestCoordinate.offset, latestCoordinate.timestamp)
            )
        )
    }

    fun latestForKey(key: String, coordinateSupplier: KFunction1<String, KafkaCoordinate>) {
        val coordinate = coordinateSupplier(key)
        val topicPartition = TopicPartition(topicName, coordinate.partition)
        expected.add(
            VerifiedCoordinate(
                topic = topicName,
                partition = topicPartition.partition(),
                offsetAndTimestamp = OffsetAndTimestamp(coordinate.offset, coordinate.timestamp),
                identifiedKey = key,
                isLatest = true
            )
        )
    }

    fun failureForCoordinate(coordinate: KafkaCoordinate, failure: Failure) {
        val topicPartition = TopicPartition(topicName, coordinate.partition)
        expected.add(
            VerifiedCoordinate(
                topic = topicName,
                partition = topicPartition.partition(),
                offsetAndTimestamp = OffsetAndTimestamp(coordinate.offset, 0),
                isFailed = true,
                failure = failure
            )
        )
    }

    fun verify(topicContext: TopicContext) {
        topicContext.verifiedCoordinates shouldContainExactly expected
    }
}

fun topicContextExpectations(
    topicName: String,
    init: TopicContextExpectations.() -> Unit
): TopicContextExpectations {
    val expectationStrategy = TopicContextExpectations(topicName)
    expectationStrategy.init()
    return expectationStrategy
}
