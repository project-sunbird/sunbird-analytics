package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.ContentActivitySummary
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext

object ContentActivitySummarizer {
  def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, ContentActivitySummary);
    }
}