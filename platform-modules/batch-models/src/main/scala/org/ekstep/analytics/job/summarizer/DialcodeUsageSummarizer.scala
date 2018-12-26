package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.model.DialcodeUsageSummaryModel

object DialcodeUsageSummarizer extends optional.Application with IJob {

  implicit val className = "org.ekstep.analytics.job.DialcodeUsageSummarizer"

  def main(config: String)(implicit sc: Option[SparkContext] = None) {
    implicit val sparkContext: SparkContext = sc.getOrElse(null)
    JobLogger.log("Started executing Job")
    JobDriver.run("batch", config, DialcodeUsageSummaryModel)
    JobLogger.log("Job Completed.")
  }
}