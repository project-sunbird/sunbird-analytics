package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD

/**
 * @author Santhosh
 */
object JobRunner {
  
    def executeBatch(jobClass: String, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]) : RDD[String] = {
        val job:IBatchModel = null;
        job.execute(events, jobParams);
    }
}