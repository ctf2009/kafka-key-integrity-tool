package com.ctf.kafka.topic.processor.context

import com.ctf.kafka.topic.extensions.actionIfTrue
import org.apache.kafka.clients.consumer.OffsetAndTimestamp
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

private val offsetAndTimestampComparator = compareBy(OffsetAndTimestamp::timestamp, OffsetAndTimestamp::offset)

data class PartitionContext(val topicPartition: TopicPartition, val offsetsOfInterest: SortedSet<Long>) {

    private val canCompleteEarly = AtomicBoolean()

    val keysForOffsetsToVerify = ConcurrentHashMap<String, SortedSet<OffsetAndTimestamp>>()
    val failedOffsetsOfInterest = ConcurrentHashMap<OffsetAndTimestamp, Failure>()


    fun recordOffsetOfInterestByKey(offsetAndTimestamp: OffsetAndTimestamp, key: String) {
        keysForOffsetsToVerify.computeIfAbsent(key) { sortedSetOf(offsetAndTimestampComparator) }
            .add(offsetAndTimestamp)
    }

    fun checkIfRecordKeyIsOfInterest(offsetAndTimestamp: OffsetAndTimestamp, key: String) {
        keysForOffsetsToVerify.computeIfPresent(key) { _, offsetsForKey ->
            offsetsForKey.also {
                it.add(offsetAndTimestamp)
            }
        }
    }

    fun failedOffsetOfInterest(offset: Long, failure: Failure) {
        failedOffsetsOfInterest[OffsetAndTimestamp(offset, 0)] = failure
    }

    fun determineCanCompleteEarly(currentOffset: Long) {
        (offsetsOfInterest.last() <= currentOffset && keysForOffsetsToVerify.isEmpty()).actionIfTrue {
            canCompleteEarly.set(true)
        }
    }

    fun canCompleteEarly(): Boolean = canCompleteEarly.get()
}
