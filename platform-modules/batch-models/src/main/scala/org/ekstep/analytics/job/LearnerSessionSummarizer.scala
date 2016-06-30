package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.model.LearnerSessionSummary
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob

/**
 * @author Santhosh
 */
object LearnerSessionSummarizer extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.LearnerSessionSummarizer"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.debug("Started executing Job", className)
        JobDriver.run("batch", config, LearnerSessionSummary);
        JobLogger.debug("Job completed.", className)
    }

}