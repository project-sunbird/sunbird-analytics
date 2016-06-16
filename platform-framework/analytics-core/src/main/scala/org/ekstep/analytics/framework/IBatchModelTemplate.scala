package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

trait IBatchModelTemplate[T,A,B,C] {
    
    def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[C] = {
        postProcess(algorithm(preProcess(events,jobParams),jobParams),jobParams)
    }
    
    def preProcess(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[A]
    
    def algorithm(events: RDD[A], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[B]
    
    def postProcess(events: RDD[B], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[C]
}