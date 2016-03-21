package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.model.LearnerSessionSummary
import org.apache.spark.SparkContext

/**
 * @author Santhosh
 */
object LearnerSessionSummarizer extends optional.Application {
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[Event]("batch", config, LearnerSessionSummary);
    }
  
}