package org.ekstep.analytics.job

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.updater.ContentUsageUpdater
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger

object ContentUsageUpdaterJob extends Application with IJob {
    
    val className = "org.ekstep.analytics.job.ContentUsageUpdaterJob"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job", className, None, None, None)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, ContentUsageUpdater);
        JobLogger.log("Job Completed.", className, None, None, None)
    }
}