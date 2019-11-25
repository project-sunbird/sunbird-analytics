package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Event, SparkSpec}

class TestKafkaDispatcher extends SparkSpec {

    "KafkaDispatcher" should "send events to kafka" in {

        events = loadFile[Event](file)
        val rdd = events.map(f => JSONUtils.serialize(f))
        KafkaDispatcher.dispatch(Map("brokerList" -> "localhost:9092", "topic" -> "test"), rdd);

        KafkaDispatcher.dispatch(rdd.collect(), Map("brokerList" -> "localhost:9092", "topic" -> "test"));

    }

    it should "give DispatcherException if broker list or topic is missing" in {

        events = loadFile[Event](file)
        val rdd = events.map(f => JSONUtils.serialize(f))
        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(Map("topic" -> "test"), rdd);
        } should have message "brokerList parameter is required to send output to kafka"

        the[DispatcherException] thrownBy {
            KafkaDispatcher.dispatch(Map("brokerList" -> "localhost:9092"), rdd);
        } should have message "topic parameter is required to send output to kafka"


    }
}
