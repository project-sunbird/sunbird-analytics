package org.ekstep.analytics.job.summarizer

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.GenieUsageSummaryModel

object GenieUsageSummarizer extends Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.summarizer.GenieUsageSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, GenieUsageSummaryModel);
    }
    
}