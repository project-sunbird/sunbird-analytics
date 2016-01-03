package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext


/**
 * @author Santhosh
 */
trait IBatchModel {
    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String]
}