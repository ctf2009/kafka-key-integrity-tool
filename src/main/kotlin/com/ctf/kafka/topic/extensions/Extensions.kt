package com.ctf.kafka.topic.extensions

import kotlinx.coroutines.Job
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

fun <R> Boolean.valueIfTrueElseNull(result: () -> R): R? {
    return if (this) {
        result()
    } else {
        null
    }
}

fun <R> Boolean.valueIfFalseElseNull(result: () -> R): R? {
    return if (!this) {
        result()
    } else {
        null
    }
}

fun Boolean.actionIfTrue(action: () -> Unit) {
    if (this) {
        action()
    }
}

fun Boolean.actionIfFalse(action: () -> Unit): Boolean {
    if (!this) {
        action()
    }
    return this
}

suspend fun Job.awaitAllChildren() {
    this.children.forEach { it.join() }
}

fun ConsumerRecord<*, *>.hasKey(): Boolean = this.key() != null

fun Consumer<*, *>.beginningOffsetsByPartitionId(topicPartitions: Collection<TopicPartition>) = this
    .beginningOffsets(topicPartitions)
    .mapKeys { it.key.partition() }

fun Consumer<*, *>.endOffsetsByPartitionId(topicPartitions: Collection<TopicPartition>) = this
    .endOffsets(topicPartitions)
    .mapKeys { it.key.partition() }
