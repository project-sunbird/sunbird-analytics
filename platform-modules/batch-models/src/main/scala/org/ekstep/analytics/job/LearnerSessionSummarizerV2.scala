package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.model.LearnerSessionSummaryV2
import org.ekstep.analytics.framework.TelemetryEventV2

/**
 * @author Santhosh
 */
object LearnerSessionSummarizerV2 extends optional.Application {
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[TelemetryEventV2]("batch", config, LearnerSessionSummaryV2);
    }
  
}