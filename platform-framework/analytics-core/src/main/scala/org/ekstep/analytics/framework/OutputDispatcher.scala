package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.exception.DispatcherException
import org.ekstep.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.factory.DispatcherFactory
import org.ekstep.analytics.framework.exception.DispatcherException

/**
 * @author Santhosh
 */
object OutputDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[String]) = {
        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = events.collect();
        if (eventArr.length == 0) {
            Console.println("### No events produced ###");
            throw new DispatcherException("No events produced for output dispatch");
        }
        outputs.get.foreach { dispatcher =>
            Console.println("### Dispatching output to - " + dispatcher.to + " ###");
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        }
    }
    
    @throws(classOf[DispatcherException])
    def dispatch(dispatcher: Dispatcher, events: RDD[String]) = {
        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = events.collect();
        if (eventArr.length == 0) {
            Console.println("### No events produced ###");
            throw new DispatcherException("No events produced for output dispatch");
        }
        Console.println("### Dispatching output to - " + dispatcher.to + " ###");
        DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
    }
}