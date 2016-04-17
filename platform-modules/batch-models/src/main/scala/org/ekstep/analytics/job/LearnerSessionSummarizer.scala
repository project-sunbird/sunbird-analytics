package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.model.LearnerSessionSummary
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object LearnerSessionSummarizer extends optional.Application {

    val className = this.getClass.getName
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.info("Started executing LearnerSessionSummarizer Job", className)
        JobDriver.run[Event]("batch", config, LearnerSessionSummary);
        JobLogger.info("LearnerSessionSummarizer Job completed....", className)
    }

}