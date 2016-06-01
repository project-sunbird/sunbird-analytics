package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.model.ContentSideloadingSummary
import org.ekstep.analytics.framework.IJob

object ContentSideloadingSummarizer extends optional.Application with IJob {
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[Event]("batch", config, ContentSideloadingSummary);
    }
}