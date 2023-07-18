package com.ctf.kafka.topic.reporting

import com.ctf.kafka.topic.processor.context.TopicContext

fun interface Reporter {
    fun report(verifiedMap: Map<String, TopicContext>)
}
