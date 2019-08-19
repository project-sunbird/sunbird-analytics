package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.DailyConsumptionReportModel

object ConsumptionReportsJob extends optional.Application with IJob {

  implicit val className: String = "org.ekstep.analytics.job.ConsumptionReportsJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None): Unit = {
    JobLogger.log("Started executing ConsumptionReportsJob")
    implicit val sparkContext: SparkContext = sc.orNull
    JobDriver.run("batch", config, DailyConsumptionReportModel)
    JobLogger.log("ConsumptionReportsJob execution completed.")
  }

}
