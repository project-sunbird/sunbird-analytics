package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateContentPopularity
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JobLogger

object ContentPopularityUpdater extends optional.Application {

    val className = "org.ekstep.analytics.job.ContentPopularityUpdater"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateContentPopularity);
        JobLogger.debug("Job Completed.", className)
    }
}