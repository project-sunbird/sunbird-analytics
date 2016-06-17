package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.ContentUsageSummary
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger

object ContentUsageSummarizer extends Application with IJob {

    val className = "org.ekstep.analytics.job.ContentUsageSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        //JobDriver.run[MeasuredEvent]("batch", config, ContentUsageSummary);
        JobLogger.debug("Job Completed.", className)
    }
}