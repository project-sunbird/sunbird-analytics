package org.ekstep.analytics.job.summarizer

import optional.Application
import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.EndOfContentRecommendationModel

object EndOfContentRecommendationJob extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.EndOfContentRecommendationJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, EndOfContentRecommendationModel);
    }
}