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
    def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[MEEvent]) = {
        println("in output dispatcher dispatch()....")
        if (outputs.isEmpty) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = getStringifiedMeasuredEvent(events).collect();
        if (eventArr.length != 0) {
            outputs.get.foreach { dispatcher =>
                JobLogger.debug("Dispatching output", className, Option(dispatcher.to));
                DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
            }
        } else {
            JobLogger.debug("No events produced for dispatch", className);
            null;
        }

    }

    @throws(classOf[DispatcherException])
    def dispatch(dispatcher: Dispatcher, events: RDD[MEEvent]) = {
        if (null == dispatcher) {
            throw new DispatcherException("No output configurations found");
        }
        val eventArr = getStringifiedMeasuredEvent(events).collect();
        if (eventArr.length != 0) {
            JobLogger.debug("Dispatching output", className, Option(dispatcher.to));
            DispatcherFactory.getDispatcher(dispatcher).dispatch(eventArr, dispatcher.params);
        } else {
            JobLogger.debug("No events produced", className);
            null;
        }
    }
    
    def getStringifiedMeasuredEvent(events: RDD[MEEvent]):RDD[String] ={
        println("in getdtringifiedME()...")
        events.map{x => 
            JSONUtils.serialize(CommonUtil.getMeasuredEvent(x))   
        }
    }
}