package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

trait IBatchModelTemplate[T,A,B] extends IBatchModel[T]{
    
    def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String] = {
        postProcess(algorithm(preProcess(events,jobParams),jobParams),jobParams)
    }
    
    def preProcess[A](events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[A]
    
    def algorithm[B](events: RDD[A], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[B]
    
    def postProcess(events: RDD[B], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String]
}