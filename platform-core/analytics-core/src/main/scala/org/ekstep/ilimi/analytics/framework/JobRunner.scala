package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
object JobRunner {
  
    def executeBatch(jobClass: String, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] = {
        val model = Class.forName(jobClass).newInstance.asInstanceOf[{ def execute(events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] }];;
        model.execute(events, jobParams);
    }
}