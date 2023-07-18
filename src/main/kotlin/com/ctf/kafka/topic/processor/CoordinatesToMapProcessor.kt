package com.ctf.kafka.topic.processor

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentHashMap

private const val TOPIC_GROUP = 1
private const val PARTITION_GROUP = 2
private const val OFFSET_GROUP = 3

private val kafkaCoordinatesRegex = Regex("^(.*)-(\\d+)-(\\d+)$")

object CoordinatesToMapProcessor {

    private val logger = KotlinLogging.logger {}

    fun CoroutineScope.processCoordinates(
        channel: ReceiveChannel<String>,
        numProcessingRoutines: Int = 2,
    ): Map<String, Map<Int, SortedSet<Long>>> {
        val topicMappings: MutableMap<String, MutableMap<Int, SortedSet<Long>>> = ConcurrentHashMap()
        repeat(numProcessingRoutines) {
            launch {
                for (coordinatesString in channel) {
                    processCoordinate(coordinatesString, topicMappings)
                }
            }
        }
        return topicMappings
    }

    private fun processCoordinate(
        coordinatesString: String, topicMappings: MutableMap<String, MutableMap<Int, SortedSet<Long>>>
    ) {
        val matchResult = kafkaCoordinatesRegex.find(coordinatesString)
            ?: throw IllegalArgumentException("Invalid Kafka coordinates format: $coordinatesString")

        val topic = matchResult.groupValues[TOPIC_GROUP]
        val partition = matchResult.groupValues[PARTITION_GROUP].toInt()
        val offset = matchResult.groupValues[OFFSET_GROUP].toLong()

        populateMapWithCoordinate(topic, partition, offset, topicMappings)
    }

    private fun populateMapWithCoordinate(
        topic: String,
        partition: Int,
        offset: Long,
        topicMappings: MutableMap<String, MutableMap<Int, SortedSet<Long>>>
    ) {
        topicMappings
            .computeIfAbsent(topic) { ConcurrentHashMap<Int, SortedSet<Long>>() }
            .computeIfAbsent(partition) { sortedSetOf() }.also { offsets ->
                if (offsets.contains(offset)) {
                    logger.warn { "Duplicate found for $topic-$partition-$offset .... Skipping" }
                } else {
                    offsets.add(offset)
                }
            }
    }
}
