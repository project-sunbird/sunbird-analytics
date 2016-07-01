package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.AserScreenSummary
import org.ekstep.analytics.framework.Event
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.IJob

/**
 * @author Santhosh
 */
object AserScreenSummarizer extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.AserScreenSummarizer"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job", className, None, None, None, "DEBUG")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, AserScreenSummary);
        JobLogger.log("Job Completed.", className, None, None, None, "DEBUG")
    }
}