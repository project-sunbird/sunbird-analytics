package org.ekstep.analytics.job

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.ekstep.analytics.model.DeviceRecommendationModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger

object DeviceRecommendationJob extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.DeviceRecommendationJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, DeviceRecommendationModel);
        JobLogger.log("Job Completed.")
    }
}