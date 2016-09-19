package org.ekstep.analytics.job

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.ekstep.analytics.model.REScoringModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger

object REScoringJob extends Application with IJob  {
   
    implicit val className = "org.ekstep.analytics.job.REScoringJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, REScoringModel);
        JobLogger.log("Job Completed.")
    }
}