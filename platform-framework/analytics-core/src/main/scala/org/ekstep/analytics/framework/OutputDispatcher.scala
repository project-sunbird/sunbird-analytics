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
import org.ekstep.analytics.framework.dispatcher.FileDispatcher

/**
 * @author Santhosh
 */
object OutputDispatcher {

    implicit val className = "org.ekstep.analytics.framework.OutputDispatcher";

    @throws(classOf[DispatcherException])
    def dispatch[T](outputs: Option[Array[Dispatcher]], events: RDD[T]): Long = {

        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events).collect();
        if (eventArr.length != 0) {
            outputs.get.foreach { dispatcher =>
                JobLogger.log("Dispatching output", Option(dispatcher.to));
                DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
            }
        } else {
            JobLogger.log("No events produced for dispatch");
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
            JobLogger.log("Dispatching output", Option(dispatcher.to));
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        } else {
            JobLogger.log("No events produced");
            null;
        }
    }
    
    @throws(classOf[DispatcherException])
    def dispatch[T](dispatcher: Dispatcher, events: Array[String]) = {

        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        if (events.length != 0) {
            JobLogger.log("Dispatching output", Option(dispatcher.to));
            DispatcherFactory.getDispatcher(dispatcher).dispatch(events, dispatcher.params);
        } else {
            JobLogger.log("No events produced");
            null;
        }
    }
    
    @throws(classOf[DispatcherException])
    def dispatchDF[T](dispatcher: Dispatcher, events: RDD[T], header: String) = {

        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = stringify(events).collect();
        if (eventArr.length != 0) {
            JobLogger.log("Dispatching output", Option(dispatcher.to));
            FileDispatcher.dispatchDF(eventArr, dispatcher.params, header);
        } else {
            JobLogger.log("No events produced");
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