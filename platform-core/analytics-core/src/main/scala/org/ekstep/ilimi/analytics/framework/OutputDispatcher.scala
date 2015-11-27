package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.exception.DispatcherException
import org.ekstep.ilimi.analytics.framework.dispatcher.S3Dispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.KafkaDispatcher
import org.ekstep.ilimi.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.ilimi.analytics.framework.factory.DispatcherFactory
import org.ekstep.ilimi.analytics.framework.exception.DispatcherException

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
        Console.println("### Events produced - " + eventArr.length + " ###");
        if (eventArr.length == 0) {
            Console.println("### No events produced ###");
            throw new DispatcherException("No events produced for output dispatch");
        }
        outputs.get.foreach { dispatcher =>
            Console.println("### Dispatching output to - " + dispatcher.to + " ###");
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        }
    }
}