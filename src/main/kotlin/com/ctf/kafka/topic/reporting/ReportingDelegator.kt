package com.ctf.kafka.topic.reporting

import com.ctf.kafka.topic.processor.context.TopicContext
import mu.KotlinLogging
import org.springframework.stereotype.Service

@Service
class ReportingDelegator(private val reporters: List<Reporter> = emptyList()) {

    private val logger = KotlinLogging.logger {}

    init {
        logger.info { "There are ${reporters.size} configured" }
    }

    fun reportResults(verifiedMap: Map<String, TopicContext>) {
        reporters.forEach { reporter -> reporter.report(verifiedMap) }
    }
}
