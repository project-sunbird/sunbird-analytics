package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.GenieLaunchSummaryModel

object GenieLaunchSummarizer extends Application with IJob {
    
    implicit val className = "org.ekstep.analytics.job.GenieLaunchSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, GenieLaunchSummaryModel);
        JobLogger.log("Job Completed.")
    }
  
}