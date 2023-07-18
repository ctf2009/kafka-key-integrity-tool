package com.ctf.kafka.topic.reporting

import com.ctf.kafka.topic.processor.context.TopicContext
import com.ctf.kafka.topic.processor.context.VerifiedCoordinate
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.function.Predicate

abstract class FilteredOffsetsReporter(
    private val outputDirectory: String,
    private val predicate: Predicate<VerifiedCoordinate>
) : Reporter {

    init {
        Files.createDirectories(Paths.get(outputDirectory))
    }

    override fun report(verifiedMap: Map<String, TopicContext>) {
        Paths.get(outputDirectory, getFilePrefix())
            .toFile()
            .printWriter().use { out ->
                verifiedMap.values
                    .flatMap { it.verifiedCoordinates }
                    .filter { predicate.test(it) }
                    .forEach { filteredCoordinate -> writeCoordinate(out, filteredCoordinate) }
            }
    }

    abstract fun getFilePrefix(): String
    abstract fun writeCoordinate(out: PrintWriter, coordinate: VerifiedCoordinate)
}

abstract class StandardLatestOffsetsReporter(
    resultsDirectory: String,
    predicate: Predicate<VerifiedCoordinate>
) : FilteredOffsetsReporter(resultsDirectory, predicate) {

    override fun writeCoordinate(out: PrintWriter, coordinate: VerifiedCoordinate) {
        out.appendLine(coordinate.toString())
    }
}

class SupersededOffsetsReporter(resultsDirectory: String) :
    StandardLatestOffsetsReporter(resultsDirectory, Predicate.not(VerifiedCoordinate::isLatest)) {

    override fun getFilePrefix(): String = FILE_PREFIX

    companion object {
        private const val FILE_PREFIX = "superseded-validated-offsets"
    }
}

class LatestOffsetsReporter(resultsDirectory: String) :
    StandardLatestOffsetsReporter(resultsDirectory, VerifiedCoordinate::isLatest) {

    override fun getFilePrefix(): String = FILE_PREFIX

    companion object {
        private const val FILE_PREFIX = "latest-validated-offsets"
    }
}

class FormattedLatestOffsetsReporter(
    outputDirectory: String,
    private val template: String
) : FilteredOffsetsReporter(outputDirectory, VerifiedCoordinate::isLatest) {
    override fun getFilePrefix(): String = FILE_PREFIX

    override fun writeCoordinate(out: PrintWriter, coordinate: VerifiedCoordinate) {
        templateMap.entries.fold(template) { acc, entry ->
            acc.replace(entry.key, entry.value(coordinate), ignoreCase = true)
        }.let { out.appendLine(it) }
    }

    companion object {
        private val templateMap = mapOf(
            "{TOPIC}" to VerifiedCoordinate::topic,
            "{PARTITION}" to { it.partition.toString() },
            "{OFFSET}" to { it.offsetAndTimestamp.offset().toString() }
        )

        private const val FILE_PREFIX = "formatted-latest-validated-offsets"
    }
}
