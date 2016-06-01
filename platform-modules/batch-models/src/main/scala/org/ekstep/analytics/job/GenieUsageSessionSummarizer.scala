package org.ekstep.analytics.job

import org.ekstep.analytics.model.GenieUsageSessionSummary
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.IJob

object GenieUsageSessionSummarizer extends Application with IJob {
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[Event]("batch", config, GenieUsageSessionSummary);
    }
}