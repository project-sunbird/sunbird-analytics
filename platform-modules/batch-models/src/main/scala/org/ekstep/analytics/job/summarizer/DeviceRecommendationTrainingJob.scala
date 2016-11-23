package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.ekstep.analytics.model.DeviceRecommendationTrainingModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger

object DeviceRecommendationTrainingJob extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.DeviceRecommendationTrainingJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, DeviceRecommendationTrainingModel);
        JobLogger.log("Job Completed.")
    }
}