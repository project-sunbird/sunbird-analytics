package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.streaming.KafkaEventProducer
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object KafkaDispatcher extends IDispatcher {

    val className = "org.ekstep.analytics.framework.dispatcher.KafkaDispatcher"

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]): Array[String] = {
        val brokerList = config.getOrElse("brokerList", null).asInstanceOf[String];
        val topic = config.getOrElse("topic", null).asInstanceOf[String];
        if (null == brokerList) {
            val msg = "brokerList parameter is required to send output to kafka"
            val exp = new DispatcherException(msg)
            JobLogger.error(msg, className, exp, None, "BE_JOB_LOG_PROCESS")
            throw exp;
        }
        if (null == topic) {
            val msg = "topic parameter is required to send output to kafka"
            val exp = new DispatcherException(msg)
            JobLogger.error(msg, className, exp, None, "BE_JOB_LOG_PROCESS")
            throw exp;
        }
        KafkaEventProducer.sendEvents(events, topic, brokerList);
        events;
    }

}