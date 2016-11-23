package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.model.ItemSummaryModel

object ItemSummarizer extends Application with IJob {
    
     implicit val className = "org.ekstep.analytics.job.summarizer.ItemSummarizer"
      
      def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, ItemSummaryModel);
    }
  
}