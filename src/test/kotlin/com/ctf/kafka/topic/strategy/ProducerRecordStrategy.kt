package com.ctf.kafka.topic.strategy

import com.ctf.kafka.topic.extensions.actionIfTrue
import com.ctf.kafka.topic.extensions.awaitAllChildren
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

data class KafkaCoordinate(
    val topic: String, val partition: Int, val offset: Long, val timestamp: Long
) : Comparable<KafkaCoordinate> {

    override fun compareTo(other: KafkaCoordinate): Int = compareBy(
        KafkaCoordinate::topic, KafkaCoordinate::partition, KafkaCoordinate::offset
    ).compare(this, other)

    override fun toString(): String {
        return "$topic-$partition-$offset"
    }
}

class ProducerRecordStrategy {
    private val listOfProducerActions =
        mutableListOf<suspend (
            KafkaTemplate<String, ByteArray>,
            Channel<Pair<CompletableFuture<SendResult<String, ByteArray>>,
                        (SendResult<String, ByteArray>) -> Unit>>
        ) -> Unit>()

    val trackedCoordinatesForKnownKeys = ConcurrentHashMap<String, SortedSet<KafkaCoordinate>>()
    private val trackedCoordinatedForMissingKeys = ConcurrentHashMap<String, KafkaCoordinate>()

    private fun getTrackedByKey(key: String) = trackedCoordinatesForKnownKeys.getValue(key)
    fun firstForKey(key: String): KafkaCoordinate = getTrackedByKey(key).first()
    fun latestForKey(key: String): KafkaCoordinate = getTrackedByKey(key).last()

    fun coordinateWithMissingKey(reference: String) = trackedCoordinatedForMissingKeys.getValue(reference)

    fun randomRecords(topic: String, numberToCreate: Int, trackCoordinates: Boolean = false) {
        listOfProducerActions.add { template, sendResultChannel ->
            logger.info { "Producing $numberToCreate random records to topic $topic" }
            repeat(numberToCreate) {
                val sendResult = template.send(ProducerRecord(topic, UUID.randomUUID().toString(), null))
                sendResultChannel.send(Pair(sendResult) {
                    trackCoordinates.actionIfTrue { trackCoordinates(it) }
                })
            }
        }
    }

    fun recordWithNoKey(topic: String, ref: String) {
        listOfProducerActions.add { template, sendResultChannel ->
            logger.info { "Producing record with no key to topic $topic. Reference to obtain coordinates is $ref" }
            val sendResult = template.send(ProducerRecord(topic, null, null))
            sendResultChannel.send(Pair(sendResult) {
                with(it.recordMetadata) {
                    trackedCoordinatedForMissingKeys[ref] =
                        KafkaCoordinate(topic, partition(), offset(), timestamp())
                }
            })
        }
    }

    fun recordWithKnownKey(
        topic: String, key: String, trackCoordinates: Boolean = true
    ) {
        logger.info { "Producing record with key $key to topic $topic" }
        listOfProducerActions.add { template, sendResultChannel ->
            val sendResult = template.send(ProducerRecord(topic, key, null))
            sendResultChannel.send(Pair(sendResult) {
                trackCoordinates.actionIfTrue { trackCoordinates(it) }
            })
        }
    }

    private fun trackCoordinates(sendResult: SendResult<String, ByteArray>) {
        with(sendResult.recordMetadata) {
            trackedCoordinatesForKnownKeys.computeIfAbsent(sendResult.producerRecord.key()) { (sortedSetOf()) }
                .add(KafkaCoordinate(topic(), partition(), offset(), timestamp()))
        }
    }

    suspend fun execute(template: KafkaTemplate<String, ByteArray>): ProducerRecordStrategy = coroutineScope {
        logger.info("Executing Producer Record Strategy")
        val channel = Channel<Pair<
                CompletableFuture<SendResult<String, ByteArray>>,
                    (SendResult<String, ByteArray>) -> Unit>>(capacity = 20_000)

        launch(Dispatchers.IO) {
            listOfProducerActions.forEach { it.invoke(template, channel) }
            channel.close()
        }

        val count = AtomicInteger()
        repeat(2) {
            launch {
                for (result in channel) {
                    result.second(withContext(Dispatchers.IO) {
                        result.first.get()
                    })
                    count.getAndIncrement().also {
                        if (it % 100_000 == 0 && it > 1) {
                            logger.info { "Confirmed $it records sent" }
                        }
                    }
                }
            }
        }
        coroutineContext[Job]?.awaitAllChildren()
        logger.info { "Processed $count records" }
        this@ProducerRecordStrategy
    }
}

fun producerRecordStrategyBuilder(init: ProducerRecordStrategy.() -> Unit): ProducerRecordStrategy {
    val producerRecordStrategy = ProducerRecordStrategy()
    producerRecordStrategy.init()
    return producerRecordStrategy
}
