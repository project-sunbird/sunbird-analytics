package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.ProfileEvent
import org.ekstep.analytics.updater.LearnerProfileUpdater
import org.ekstep.analytics.framework.IJob

object LearnerProfileUpdaterJob extends optional.Application {

    val className = "org.ekstep.analytics.job.LearnerProfileUpdaterJob"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[ProfileEvent]("batch", config, LearnerProfileUpdater);
        JobLogger.debug("Job Completed.", className)
    }
}