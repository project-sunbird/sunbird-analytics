package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.model.ETBCoverageSummaryModel

object ETBCoverageSummarizer extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.ETBCoverageSummarizer"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null)
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, ETBCoverageSummaryModel)
    JobLogger.log("Job Completed.")
  }
}