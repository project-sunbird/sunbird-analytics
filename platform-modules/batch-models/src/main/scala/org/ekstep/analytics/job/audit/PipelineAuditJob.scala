package org.ekstep.analytics.job.audit

import org.apache.spark.SparkContext
import org.ekstep.analytics.auditmodel.{PipelineAuditTask}
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{IJob, JobDriver}

object PipelineAuditJob extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.audit.SimpleAuditJob"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    JobLogger.log("Started executing Job")
    implicit val sparkContext: SparkContext = sc.orNull
    JobDriver.run("batch", config, PipelineAuditTask)
    JobLogger.log("Job Completed.")
  }
}
