package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.MEEvent
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.model.CAS
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.ContentSumm
import scala.collection.mutable.Buffer

object ContentActivitySummarizer extends optional.Application {

    val className = "org.ekstep.analytics.job.ContentActivitySummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        println("in CAS job");
        JobLogger.debug("Started executing Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MEEvent]("batch", config, CAS);
        JobLogger.debug("Job Completed.", className)
    }
}