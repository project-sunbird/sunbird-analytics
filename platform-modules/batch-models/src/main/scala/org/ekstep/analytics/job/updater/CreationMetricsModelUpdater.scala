package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateContentModel
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.updater.CreationMetricsUpdater

object CreationMetricsModelUpdater extends optional.Application with IJob {

     implicit val className = "org.ekstep.analytics.job.updater.CreationMetricsModelUpdater"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, CreationMetricsUpdater);
        JobLogger.log("Job Completed.")
    }
}