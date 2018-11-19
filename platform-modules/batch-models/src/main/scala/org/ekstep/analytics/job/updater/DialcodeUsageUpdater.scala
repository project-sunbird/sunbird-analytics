package org.ekstep.analytics.job.updater

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.updater.UpdateDialcodeUsageDB

object DialcodeUsageUpdater extends optional.Application with IJob {
  override def main(config: String)(implicit sc: Option[SparkContext]): Unit = {
    implicit val sparkContext: SparkContext = sc.getOrElse(null)
    JobDriver.run("batch", config, UpdateDialcodeUsageDB)
  }
}