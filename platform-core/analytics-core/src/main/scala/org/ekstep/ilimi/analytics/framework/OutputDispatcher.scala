package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.ilimi.analytics.framework.factory.DispatcherFactory

/**
 * @author Santhosh
 */
object OutputDispatcher {

    def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[String]) = {
        val eventArr = events.collect();
        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        if (eventArr.length == 0) {
            throw new DispatcherException("No events produced for output dispatch");
        }
        outputs.get.foreach { dispatcher =>
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        }
    }
}