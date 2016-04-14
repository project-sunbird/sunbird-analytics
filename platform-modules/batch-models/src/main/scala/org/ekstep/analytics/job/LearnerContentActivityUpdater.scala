package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.updater.LearnerContentActivitySummary
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

object LearnerContentActivityUpdater extends optional.Application {

    val logger = Logger.getLogger(JobLogger.jobName)
    logger.setLevel(JobLogger.level)
    val className = this.getClass.getName

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.info(logger, "Started executing LearnerContentActivityUpdater Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, LearnerContentActivitySummary);
        JobLogger.info(logger, "LearnerContentActivityUpdater Job completed.....", className)
    }
}