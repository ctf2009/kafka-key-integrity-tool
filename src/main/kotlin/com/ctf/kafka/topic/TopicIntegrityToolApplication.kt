package com.ctf.kafka.topic

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TopicIntegrityToolApplication

fun main(args: Array<String>) {
    runApplication<TopicIntegrityToolApplication>(args = args)
}
