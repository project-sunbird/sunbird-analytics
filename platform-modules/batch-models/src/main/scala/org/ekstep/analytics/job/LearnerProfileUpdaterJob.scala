package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.ProfileEvent
import org.ekstep.analytics.updater.LearnerProfileUpdater
import org.ekstep.analytics.framework.IJob

object LearnerProfileUpdaterJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.LearnerProfileUpdaterJob"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, LearnerProfileUpdater);
        JobLogger.log("Job Completed.")
    }
}