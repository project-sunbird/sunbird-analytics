package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.DeviceSpecification
import org.ekstep.analytics.framework.Event
import org.apache.spark.SparkContext
import optional.Application
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob

object DeviceSpecificationUpdater extends Application with IJob {
  
    val className = "org.ekstep.analytics.job.DeviceSpecificationUpdater"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job", className, None, None, None)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, DeviceSpecification);
        JobLogger.log("Job Completed.", className, None, None, None)
    }
}