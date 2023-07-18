package com.ctf.kafka.topic.config.properties

import jakarta.validation.constraints.Min
import jakarta.validation.constraints.NotNull
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "pooling")
data class PoolingProperties(

    @field:NotNull
    val consumerPool: CommonPoolingProperties
)

data class CommonPoolingProperties(

    @field:Min(1)
    val maxPoolSize: Int = 1
)
