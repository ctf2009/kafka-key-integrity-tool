package com.ctf.kafka.topic.config

import com.ctf.kafka.topic.config.properties.PoolingProperties
import com.ctf.kafka.topic.pooling.ResourcePool
import org.apache.kafka.clients.consumer.Consumer
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.ConsumerFactory

@Configuration
@EnableConfigurationProperties(PoolingProperties::class)
class ConsumerPoolConfiguration {

    @Bean
    fun consumerResourcePool(
        consumerFactory: ConsumerFactory<String, *>,
        poolingProperties: PoolingProperties
    ): ResourcePool<Consumer<String, *>> =
        ResourcePool(
            poolName = "Consumer-Resource-Pool",
            supplier = { consumerFactory.createConsumer() },
            maxPoolSize = poolingProperties.consumerPool.maxPoolSize
        )
}
