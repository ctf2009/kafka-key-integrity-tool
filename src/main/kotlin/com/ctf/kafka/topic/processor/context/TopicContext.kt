package com.ctf.kafka.topic.processor.context

import com.ctf.kafka.topic.extensions.actionIfFalse
import com.ctf.kafka.topic.extensions.valueIfFalseElseNull
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import java.util.*

private val logger = KotlinLogging.logger {}

data class TopicContext(
    val topicName: String,
    val availablePartitionsById: Map<Int, TopicPartition>,
    private val earliestOffsetsByPartition: Map<Int, Long>,
    private val latestWorkingByOffsets: Map<Int, Long>
) {

    val verifiedCoordinates: MutableList<VerifiedCoordinate> = mutableListOf()

    fun offsetToSearchTo(topicPartition: TopicPartition) = latestWorkingByOffsets.getValue(topicPartition.partition())

    fun isOffsetWithinRange(topicPartition: TopicPartition, offset: Long): Boolean {
        val partitionId = topicPartition.partition()
        return offset >= earliestOffsetsByPartition.getValue(partitionId)
                && offset < latestWorkingByOffsets.getValue(partitionId)
    }

    fun availablePartitionIds() = availablePartitionsById.keys

    fun verifiedCoordinates(verifiedOffsets: List<VerifiedCoordinate>) =
        verifiedCoordinates.addAll(verifiedOffsets)

    fun processCompletePartitionFailures(
        failedPartitions: Set<Int>,
        offsetsByPartition: Map<Int, SortedSet<Long>>,
        failure: Failure
    ) {
        failedPartitions.forEach { failedPartition ->
            verifiedCoordinates.addAll(
                offsetsByPartition.getValue(failedPartition)
                    .map { VerifiedCoordinate.verifiedFailed(topicName, failedPartition, it, failure) })
        }
    }

    companion object {
        fun emptyContext(topicName: String) = TopicContext(topicName, emptyMap(), emptyMap(), emptyMap())
    }
}

enum class Failure {
    OFFSET_NOT_WITHIN_RANGE, OFFSET_MISSING_KEY, PARTITION_NOT_EXISTS, TOPIC_NOT_EXISTS
}

data class VerifiedCoordinate(
    val topic: String,
    val partition: Int,
    val offsetAndTimestamp: OffsetAndTimestamp,
    val isFailed: Boolean = false,
    val failure: Failure? = null,
    val identifiedKey: String? = null,
    var isLatest: Boolean = false,
    val otherIdentifiedLatest: OffsetAndTimestamp? = null
) {


    override fun toString(): String = "$topic-$partition-${offsetAndTimestamp.offset()}"

    companion object {
        fun verifiedFailed(topic: String, partition: Int, offset: Long, failure: Failure) =
            verifiedFailed(topic, partition, OffsetAndTimestamp(offset, 0), failure)

        fun verifiedFailed(
            partitionContext: PartitionContext,
            offsetAndTimestamp: OffsetAndTimestamp,
            failure: Failure
        ) =
            verifiedFailed(
                partitionContext.topicPartition.topic(),
                partitionContext.topicPartition.partition(),
                offsetAndTimestamp,
                failure
            )

        private fun verifiedFailed(
            topic: String,
            partition: Int,
            offsetAndTimestamp: OffsetAndTimestamp,
            failure: Failure
        ) =
            VerifiedCoordinate(
                topic = topic,
                partition = partition,
                offsetAndTimestamp = offsetAndTimestamp,
                isFailed = true, failure = failure
            )

        fun verified(
            partitionContext: PartitionContext,
            offsetAndTimestamp: OffsetAndTimestamp,
            identifiedKey: String,
            latestForKey: OffsetAndTimestamp
        ): VerifiedCoordinate {
            val isLatest = (offsetAndTimestamp == latestForKey).actionIfFalse {
                logger.info {
                    "${partitionContext.topicPartition}-${offsetAndTimestamp.offset()} " +
                            "is not the latest for key $identifiedKey"
                }
            }

            return VerifiedCoordinate(
                topic = partitionContext.topicPartition.topic(),
                partition = partitionContext.topicPartition.partition(),
                offsetAndTimestamp = offsetAndTimestamp,
                identifiedKey = identifiedKey,
                isLatest = isLatest,
                otherIdentifiedLatest = isLatest.valueIfFalseElseNull { latestForKey })
        }
    }
}
