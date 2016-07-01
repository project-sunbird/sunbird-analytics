package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.LearnerProficiencySummary
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.IJob

object ProficiencyUpdater extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ProficiencyUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, LearnerProficiencySummary);
        JobLogger.log("Job Completed.")
    }
}