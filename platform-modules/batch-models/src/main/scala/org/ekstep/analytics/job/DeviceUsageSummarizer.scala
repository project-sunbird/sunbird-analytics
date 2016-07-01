package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.DeviceUsageSummary
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger

object DeviceUsageSummarizer extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DeviceUsageSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, DeviceUsageSummary);
        JobLogger.log("Job Completed")
    }
}