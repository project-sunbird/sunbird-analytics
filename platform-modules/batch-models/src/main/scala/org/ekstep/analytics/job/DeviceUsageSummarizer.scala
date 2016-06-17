package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.DeviceUsageSummary
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger

object DeviceUsageSummarizer extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.DeviceUsageSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        //JobDriver.run[MeasuredEvent]("batch", config, DeviceUsageSummary);
        JobLogger.debug("Job Completed", className)
    }
}