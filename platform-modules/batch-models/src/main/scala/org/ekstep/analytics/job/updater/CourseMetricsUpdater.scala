package org.ekstep.analytics.job.updater

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateCourseMetrics

object CourseMetricsUpdater extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.updater.CourseMetricsUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateCourseMetrics);
        JobLogger.log("Job Completed.")
    }
}