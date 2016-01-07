package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */
object JobRunner {
  
    def executeBatch[T](jobClass: String, sc: SparkContext, events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit mf:Manifest[T]) : RDD[String] = {
        Console.println("### Executing the job - " + jobClass + " ###");
        val model = Class.forName(jobClass).newInstance.asInstanceOf[{ def execute(sc: SparkContext, events: RDD[T], jobParams: Option[Map[String, AnyRef]]) : RDD[String] }];;
        model.execute(sc, events, jobParams);
    }
}