package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.model.LearnerSessionSummaryV2
import org.ekstep.analytics.framework.TelemetryEventV2
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

/**
 * @author Santhosh
 */
object LearnerSessionSummarizerV2 extends optional.Application {

    val className = "org.ekstep.analytics.job.LearnerSessionSummarizerV2"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.info("Started executing LearnerSessionSummarizerV2 Job", className)
        JobDriver.run[TelemetryEventV2]("batch", config, LearnerSessionSummaryV2);
        JobLogger.info("LearnerSessionSummarizerV2 Job completed", className)
    }

}