package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.LearnerProficiencySummaryModel

object ProficiencyUpdater extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ProficiencyUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, LearnerProficiencySummaryModel);
        JobLogger.log("Job Completed.")
    }
}