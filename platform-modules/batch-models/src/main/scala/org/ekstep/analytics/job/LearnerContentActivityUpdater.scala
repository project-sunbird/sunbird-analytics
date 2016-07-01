package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.updater.LearnerContentActivitySummary
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.IJob

object LearnerContentActivityUpdater extends optional.Application with IJob{

    implicit val className = "org.ekstep.analytics.job.LearnerContentActivityUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, LearnerContentActivitySummary);
        JobLogger.log("Job Completed.")
    }
}