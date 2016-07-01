package org.ekstep.analytics.job

import org.ekstep.analytics.model.GenieUsageSessionSummary
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger

object GenieUsageSessionSummarizer extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.GenieUsageSessionSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, GenieUsageSessionSummary);
        JobLogger.log("Job Completed.")
    }
}