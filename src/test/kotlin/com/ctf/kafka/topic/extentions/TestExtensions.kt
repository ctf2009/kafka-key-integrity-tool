package com.ctf.kafka.topic.extentions

import com.ctf.kafka.topic.strategy.KafkaCoordinate
import java.util.*

fun MutableMap<Int, SortedSet<Long>>.addCoordinateToInputMap(coordinate: KafkaCoordinate) {
    this.computeIfAbsent(coordinate.partition) { sortedSetOf() }
        .add(coordinate.offset)
}
