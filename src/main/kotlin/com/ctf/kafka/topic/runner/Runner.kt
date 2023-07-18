package com.ctf.kafka.topic.runner

import com.ctf.kafka.topic.config.properties.PoolingProperties
import com.ctf.kafka.topic.extensions.awaitAllChildren
import com.ctf.kafka.topic.processor.CoordinatesToMapProcessor.processCoordinates
import com.ctf.kafka.topic.processor.PartitionScopedKeyProcessor
import com.ctf.kafka.topic.processor.context.TopicContext
import com.ctf.kafka.topic.reporting.ReportingDelegator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.springframework.boot.CommandLineRunner
import java.io.File
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.exists
import kotlin.io.path.isReadable
import kotlin.io.path.isRegularFile

@OptIn(ExperimentalCoroutinesApi::class)
class Runner(
    private val topicIntegrityProcessor: PartitionScopedKeyProcessor,
    private val poolingProperties: PoolingProperties,
    private val reporting: ReportingDelegator
) : CommandLineRunner {

    private val logger = KotlinLogging.logger {}

    override fun run(args: Array<String>): Unit = runBlocking {
        val file = getFileFromArgs(args)

        val processedMapFromFile = buildKafkaCoordinatesMapFromFile(file)
        reporting.reportResults(processTopicMap(processedMapFromFile))

        logger.info { "Completed" }
    }

    private fun getFileFromArgs(args: Array<out String?>): File {
        if (args.isEmpty()) {
            error("A filename is required to be provided as an argument")
        }

        val path = Paths.get(args[0]!!)

        logger.info { "Verifying path: $path" }
        check(path.exists()) { "Path $path does not exist" }
        check(path.isRegularFile()) { "Path $path is not a file" }
        check(path.isReadable()) { "Path $path is not readable" }

        return path.toFile()
    }

    private suspend fun buildKafkaCoordinatesMapFromFile(file: File): Map<String, Map<Int, SortedSet<Long>>> =
        coroutineScope {
            val channel = produce(
                capacity = 100
            ) {
                logger.info { "Processing File" }
                file.bufferedReader().useLines { line ->
                    line.filter(CharSequence::isNotEmpty).forEach { string -> send(string) }
                }
            }

            processCoordinates(channel)
        }

    private suspend fun processTopicMap(topicMap: Map<String, Map<Int, SortedSet<Long>>>): Map<String, TopicContext> =
        coroutineScope {
            val context = Job() + Dispatchers.IO
            val channel = produce(capacity = 10, context = context) {
                topicMap.forEach {
                    send(it)
                }
            }

            val verifiedTopicMap = ConcurrentHashMap<String, TopicContext>()
            repeat(topicMap.size.coerceAtMost(poolingProperties.consumerPool.maxPoolSize)) {
                logger.debug { "Launching Topic Integrity Routine with Id: $it" }
                launch(context) {
                    for (topicData in channel) {
                        logger.info { "Coroutine $it is processing topic ${topicData.key}" }
                        verifiedTopicMap[topicData.key] =
                            topicIntegrityProcessor.processTopic(topicData.key, topicData.value)
                    }
                }
            }

            context.job.awaitAllChildren()
            verifiedTopicMap
        }
}
