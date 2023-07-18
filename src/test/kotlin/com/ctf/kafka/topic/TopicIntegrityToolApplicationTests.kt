package com.ctf.kafka.topic

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(properties = ["runner.enabled=false"])
@ActiveProfiles("Test")
class TopicIntegrityToolApplicationTests {

    @Test
    @Suppress("EmptyFunctionBlock")
    fun contextLoads() {
        // Check context loads
    }
}
