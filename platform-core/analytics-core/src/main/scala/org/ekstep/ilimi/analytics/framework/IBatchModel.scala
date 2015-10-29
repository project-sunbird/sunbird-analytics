package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD


/**
 * @author Santhosh
 */
trait IBatchModel {
    def execute(events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String]
}