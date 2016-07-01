package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.util.JobLogger
import scala.collection.mutable.Buffer
import org.ekstep.analytics.model.ContentActivitySummary

object ContentActivitySummarizer extends optional.Application {

    val className = "org.ekstep.analytics.job.ContentActivitySummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job", className, None, None, None, "DEBUG")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, ContentActivitySummary);
        JobLogger.log("Job Completed.", className, None, None, None, "DEBUG")
    }
}