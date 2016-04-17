package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.AserScreenSummary
import org.ekstep.analytics.framework.Event
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger

/**
 * @author Santhosh
 */
object AserScreenSummarizer extends optional.Application {

    val className = this.getClass.getName
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.info("Started executing AserScreenSummarizer Job", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[Event]("batch", config, AserScreenSummary);
        JobLogger.info("AserScreenSummarizer Job Completed....", className)
    }

}