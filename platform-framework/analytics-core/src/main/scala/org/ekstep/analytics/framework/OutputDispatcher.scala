package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.factory.DispatcherFactory
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object OutputDispatcher {

    val className = "org.ekstep.analytics.framework.OutputDispatcher"
    @throws(classOf[DispatcherException])
    def dispatch[T](outputs: Option[Array[Dispatcher]], events: RDD[T]) : Long = {
        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events).collect();
        if (eventArr.length != 0) {
            outputs.get.foreach { dispatcher =>
                JobLogger.log("Dispatching output", className, None, Option(dispatcher.to), None, "DEBUG");
                DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
            }
        } else {
            JobLogger.log("No events produced for dispatch", className, None, None, None, "DEBUG");
        }
        eventArr.length;

    }

    @throws(classOf[DispatcherException])
    def dispatch[T](dispatcher: Dispatcher, events: RDD[T]) = {
        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events).collect();
        if (eventArr.length != 0) {
            JobLogger.log("Dispatching output", className, None, Option(dispatcher.to), None, "DEBUG");
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        } else {
            JobLogger.log("No events produced", className, None, None, None, "DEBUG");
            null;
        }
    }

    def stringify[T](events: RDD[T]): RDD[String] = {
        events.map { x =>
            if (x.isInstanceOf[String])
                x.asInstanceOf[String]
            else
                JSONUtils.serialize(x.asInstanceOf[AnyRef])
        }
    }
}