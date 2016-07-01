package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.GenieLaunchSummary
import org.ekstep.analytics.framework.util.JobLogger

object GenieLaunchSummarizer extends Application with IJob {
    
    val className = "org.ekstep.analytics.job.GenieLaunchSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job", className, None, None, None)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, GenieLaunchSummary);
        JobLogger.log("Job Completed.", className, None, None, None)
    }
  
}