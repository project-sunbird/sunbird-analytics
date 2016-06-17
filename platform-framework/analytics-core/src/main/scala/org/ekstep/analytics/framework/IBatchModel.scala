package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


/**
 * @author Santhosh
 */
trait IBatchModel[T,A,B,MEEvent] {
    def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[MEEvent] = {
        postProcess(algorithm(preProcess(events,jobParams),jobParams),jobParams)
    }
    
    def preProcess(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[A]
    
    def algorithm(events: RDD[A], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[B]
    
    def postProcess(events: RDD[B], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[MEEvent]
}