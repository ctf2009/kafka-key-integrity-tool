package com.ctf.kafka.topic.config

import com.ctf.kafka.topic.reporting.FormattedLatestOffsetsReporter
import com.ctf.kafka.topic.reporting.LatestOffsetsReporter
import com.ctf.kafka.topic.reporting.Reporter
import com.ctf.kafka.topic.reporting.SupersededOffsetsReporter
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class ReportingConfiguration {

    @Bean
    @ConditionalOnProperty(
        prefix = "reporting.latest-only-coordinates",
        value = ["enabled"],
        havingValue = "true",
    )
    fun latestOnlyReporter(
        @Value("\${reporting.output-directory}") outputDirectory: String
    ): Reporter =
        LatestOffsetsReporter(outputDirectory)

    @Bean
    @ConditionalOnProperty(
        prefix = "reporting.superseded-offsets",
        value = ["enabled"],
        havingValue = "true",
    )
    fun supersededOnlyReporter(
        @Value("\${reporting.output-directory}") outputDirectory: String
    ): Reporter =
        SupersededOffsetsReporter(outputDirectory)

    @Bean
    @ConditionalOnProperty(
        prefix = "reporting.formatted-latest-only-coordinates",
        value = ["enabled"],
        havingValue = "true",
    )
    fun formattedLatestOnlyReporter(
        @Value("\${reporting.output-directory}") outputDirectory: String,
        @Value("\${reporting.formatted-latest-only-coordinates.template}") template: String
    ): Reporter = FormattedLatestOffsetsReporter(outputDirectory, template)


    @Bean
    @ConditionalOnMissingBean
    fun defaultReporter(
        @Value("\${reporting.output-directory}") resultsDir: String
    ): Reporter = LatestOffsetsReporter(resultsDir)
}
