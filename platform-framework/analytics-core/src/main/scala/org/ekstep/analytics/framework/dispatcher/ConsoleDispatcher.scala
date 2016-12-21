package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
object ConsoleDispatcher extends IDispatcher {

    def dispatch(events: RDD[String], config: Map[String, AnyRef]) {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                println("Event", event);
            }
        }
        val eventArr = events.collect
        println("Total Events Size", eventArr.length);
    }
    
    def dispatch(events: Array[String], config: Map[String, AnyRef]) {
        if (config.getOrElse("printEvent", true).asInstanceOf[Boolean]) {
            for (event <- events) {
                println("Event", event);
            }
        }
        val eventArr = events
        println("Total Events Size", eventArr.length);
    }
}