package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.model.LearnerSessionSummary
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.TelemetryEventV2
import org.ekstep.analytics.model.LearnerSessionSummaryV2
import org.ekstep.analytics.framework.IJob

/**
 * @author Santhosh
 */
object LearnerSessionSummarizer extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.LearnerSessionSummarizer"
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.info("Started executing LearnerSessionSummarizer Job", className)
        JobDriver.run[Event]("batch", config, LearnerSessionSummary);
        JobDriver.run[TelemetryEventV2]("batch", config, LearnerSessionSummaryV2);
        JobLogger.info("LearnerSessionSummarizer Job completed....", className)
    }

}