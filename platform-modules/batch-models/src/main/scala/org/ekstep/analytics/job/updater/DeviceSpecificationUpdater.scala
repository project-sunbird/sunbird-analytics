package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.UpdateDeviceSpecificationDB
import org.apache.spark.SparkContext
import optional.Application
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob

object DeviceSpecificationUpdater extends Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DeviceSpecificationUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, UpdateDeviceSpecificationDB);
        JobLogger.log("Job Completed.")
    }
}