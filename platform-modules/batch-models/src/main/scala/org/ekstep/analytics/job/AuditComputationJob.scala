package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.AuditComputationModel

object AuditComputationJob extends optional.Application with IJob {

  implicit val className: String = "org.ekstep.analytics.job.AuditComputationJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None): Unit = {
    JobLogger.log("Started executing AuditComputationJob")
    implicit val sparkContext: SparkContext = sc.orNull
    JobDriver.run("batch", config, AuditComputationModel)
    JobLogger.log("AuditComputationJob execution completed.")
  }

}
