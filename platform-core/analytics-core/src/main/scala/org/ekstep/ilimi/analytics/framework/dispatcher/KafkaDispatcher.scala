package org.ekstep.ilimi.analytics.framework.dispatcher

import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.streaming.KafkaEventProducer

/**
 * @author Santhosh
 */
object KafkaDispatcher {

    def outputToKafka(brokerList: String, topic: String, events: Array[String]) = {
        if (null == brokerList) {
            throw new DispatcherException("brokerList parameter is required to send output to kafka");
        }
        if (null == topic) {
            throw new DispatcherException("topic parameter is required to send output to kafka");
        }
        KafkaEventProducer.sendEvents(events, topic, brokerList)
    }

}