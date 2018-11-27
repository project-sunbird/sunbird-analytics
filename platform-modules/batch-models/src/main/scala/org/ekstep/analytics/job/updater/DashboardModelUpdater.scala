package org.ekstep.analytics.job.updater

/**
  * @author Manjunath Davanam <manjunathd@ili.in>
  */

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.UpdateDashboardModel

object DashboardModelUpdater extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.DashboardModelUpdater"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null);
    JobLogger.log("Started executing UpdateDashboardModel job")
    JobDriver.run("batch", config, UpdateDashboardModel)
    JobLogger.log("UpdateDashboardModel Job Completed!!")
  }
}