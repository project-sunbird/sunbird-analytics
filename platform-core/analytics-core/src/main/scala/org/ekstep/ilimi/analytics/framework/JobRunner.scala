package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */
object JobRunner {
  
    def executeBatch(jobClass: String, sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] = {
        val model = Class.forName(jobClass).newInstance.asInstanceOf[{ def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] }];;
        model.execute(sc, events, jobParams);
    }
}