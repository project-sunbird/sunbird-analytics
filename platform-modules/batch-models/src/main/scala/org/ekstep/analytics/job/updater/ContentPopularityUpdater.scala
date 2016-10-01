package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateContentPopularityDB

object ContentPopularityUpdater extends Application with IJob {

	implicit val className = "org.ekstep.analytics.job.ContentPopularityUpdater"
	
	def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateContentPopularityDB);
        JobLogger.log("Job Completed.")
    }
}