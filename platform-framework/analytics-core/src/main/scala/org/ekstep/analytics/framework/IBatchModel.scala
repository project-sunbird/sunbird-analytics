package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


/**
 * @author Santhosh
 */
trait IBatchModel[T] {
    def execute(sc: SparkContext, events: RDD[T], jobParams: Option[Map[String, AnyRef]]) : RDD[String]
}