package org.ekstep.analytics.job

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.updater.ContentUsageUpdater
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver

object ContentUsageUpdaterJob extends Application with IJob {
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, ContentUsageUpdater);
    }
}