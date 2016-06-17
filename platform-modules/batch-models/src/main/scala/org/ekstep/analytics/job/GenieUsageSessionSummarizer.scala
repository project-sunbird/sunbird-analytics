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
  
    val className = "org.ekstep.analytics.job.GenieUsageSessionSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        //JobDriver.run[Event]("batch", config, GenieUsageSessionSummary);
        JobLogger.debug("Job Completed.", className)
    }
}