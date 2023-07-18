package com.ctf.kafka.topic.config

import com.ctf.kafka.topic.config.properties.PoolingProperties
import com.ctf.kafka.topic.processor.PartitionScopedKeyProcessor
import com.ctf.kafka.topic.reporting.ReportingDelegator
import com.ctf.kafka.topic.runner.Runner
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class CommandLineRunnerConfig {

    @ConditionalOnProperty(prefix = "runner", value = ["enabled"], havingValue = "true", matchIfMissing = true)
    @Bean
    fun commandLineRunner(
        topicIntegrityProcessor: PartitionScopedKeyProcessor,
        poolingProperties: PoolingProperties,
        reporting: ReportingDelegator
    ): CommandLineRunner = Runner(topicIntegrityProcessor, poolingProperties, reporting)
}
