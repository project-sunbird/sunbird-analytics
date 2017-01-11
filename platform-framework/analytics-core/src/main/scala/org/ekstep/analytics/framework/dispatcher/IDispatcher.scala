package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
trait IDispatcher {

    @throws(classOf[DispatcherException])
    def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String];
    
    @throws(classOf[DispatcherException])
    def dispatch(config: Map[String, AnyRef], events: RDD[String])(implicit sc: SparkContext);
    
}