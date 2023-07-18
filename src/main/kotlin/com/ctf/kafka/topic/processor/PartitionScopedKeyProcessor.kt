package com.ctf.kafka.topic.processor

import com.ctf.kafka.topic.extensions.actionIfFalse
import com.ctf.kafka.topic.extensions.beginningOffsetsByPartitionId
import com.ctf.kafka.topic.extensions.endOffsetsByPartitionId
import com.ctf.kafka.topic.extensions.hasKey
import com.ctf.kafka.topic.pooling.ResourcePool
import com.ctf.kafka.topic.processor.context.Failure
import com.ctf.kafka.topic.processor.context.PartitionContext
import com.ctf.kafka.topic.processor.context.TopicContext
import com.ctf.kafka.topic.processor.context.VerifiedCoordinate
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.*

private const val POLL_TIMEOUT_SECONDS: Long = 5

@Component
class PartitionScopedKeyProcessor(private val consumerPool: ResourcePool<Consumer<String, *>>) {

    private val logger = KotlinLogging.logger {}

    suspend fun processTopic(topicName: String, offsetsByPartitionId: Map<Int, SortedSet<Long>>): TopicContext {
        val consumer = consumerPool.acquireResource()

        try {
            val topicPartitions = consumer.listTopics(Duration.ofSeconds(1))
            if (!topicPartitions.containsKey(topicName)) {
                return TopicContext.emptyContext(topicName).also {
                    it.processCompletePartitionFailures(
                        offsetsByPartitionId.keys,
                        offsetsByPartitionId,
                        Failure.TOPIC_NOT_EXISTS
                    )
                }
            }

            val availablePartitionsById = topicPartitions.getValue(topicName)
                .associateBy { it.partition() }
                .mapValues { TopicPartition(topicName, it.value.partition()) }
                .filterKeys { offsetsByPartitionId.containsKey(it) }

            val topicContext = TopicContext(
                topicName = topicName,
                availablePartitionsById = availablePartitionsById,
                earliestOffsetsByPartition = consumer.beginningOffsetsByPartitionId(availablePartitionsById.values),
                latestWorkingByOffsets = consumer.endOffsetsByPartitionId(availablePartitionsById.values)
            )

            offsetsByPartitionId.keys.minus(topicContext.availablePartitionIds()).also { missingPartitions ->
                if (missingPartitions.isNotEmpty()) {
                    logger.warn { "Some partitions do not exist for the topic [$topicName]: $missingPartitions" }
                }
                topicContext.processCompletePartitionFailures(
                    missingPartitions,
                    offsetsByPartitionId,
                    Failure.PARTITION_NOT_EXISTS
                )
            }

            topicContext.availablePartitionsById.forEach { (id, topicPartition) ->
                logger.info { "Processing TopicPartition $topicPartition" }
                val verified =
                    processPartition(topicPartition, topicContext, offsetsByPartitionId.getValue(id), consumer)

                topicContext.verifiedCoordinates(verified)
            }

            return topicContext
        } finally {
            consumerPool.releaseResource(consumer)
        }
    }

    private fun processPartition(
        topicPartition: TopicPartition,
        topicContext: TopicContext,
        offsetsOfInterest: SortedSet<Long>,
        consumer: Consumer<String, *>
    ): List<VerifiedCoordinate> {
        consumer.assign(listOf(topicPartition))
        val partitionContext = PartitionContext(topicPartition, offsetsOfInterest)
        val earliestOffsetToSeek = offsetsOfInterest.find { offset ->
            topicContext.isOffsetWithinRange(topicPartition.partition(), offset).actionIfFalse {
                partitionContext.failedOffsetOfInterest(offset, Failure.OFFSET_NOT_WITHIN_RANGE)
            }
        }

        earliestOffsetToSeek?.let {
            consumePartition(it, consumer, topicContext, partitionContext)
            logger.debug { "Completed processing partition at offset ${consumer.position(topicPartition)}" }
        }
        return buildVerifiedOffsetList(partitionContext)
    }

    private fun consumePartition(
        startingOffset: Long,
        consumer: Consumer<String, *>,
        topicContext: TopicContext,
        partitionContext: PartitionContext
    ) {

        val topicPartition = partitionContext.topicPartition
        consumer.seek(topicPartition, startingOffset)
        consumeLoop@ while (consumer.position(topicPartition) < topicContext.offsetToSearchTo(topicPartition)) {
            for (record in consumer.poll(Duration.ofSeconds(POLL_TIMEOUT_SECONDS))) {
                if (partitionContext.canCompleteEarly()) {
                    logger.info {
                        "Completing partition ${topicPartition.partition()} early as no keys left to check"
                    }
                    break@consumeLoop
                }
                consumeAction(partitionContext, record)
            }
        }
    }

    private fun consumeAction(partitionContext: PartitionContext, record: ConsumerRecord<String, *>) {
        val offsetAndTimestamp = OffsetAndTimestamp(record.offset(), record.timestamp())
        if (partitionContext.offsetsOfInterest.contains(record.offset())) {
            if (record.hasKey()) {
                partitionContext.recordOffsetOfInterestByKey(offsetAndTimestamp, record.key())
            } else {
                partitionContext.failedOffsetOfInterest(record.offset(), Failure.OFFSET_MISSING_KEY)
                partitionContext.determineCanCompleteEarly(record.offset())
            }
        } else {
            partitionContext.checkIfRecordKeyIsOfInterest(offsetAndTimestamp, record.key())
        }
    }

    private fun buildVerifiedOffsetList(
        partitionContext: PartitionContext
    ): MutableList<VerifiedCoordinate> =
        mutableListOf<VerifiedCoordinate>().apply {
            partitionContext.keysForOffsetsToVerify.forEach { (key, offsets) ->
                val latestForKey = offsets.last()
                offsets
                    .filter { partitionContext.offsetsOfInterest.contains(it.offset()) }
                    .forEach { add(VerifiedCoordinate.verified(partitionContext, it, key, latestForKey)) }
            }

            partitionContext.failedOffsetsOfInterest.forEach { (offset, failure) ->
                add(VerifiedCoordinate.verifiedFailed(partitionContext, offset, failure))
            }
        }
}
