package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.ContentUsageSummary
import org.ekstep.analytics.framework.IJob

object ContentUsageSummarizer extends Application with IJob {

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, ContentUsageSummary);
    }
}