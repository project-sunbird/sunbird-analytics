package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.RecommendationEngine
import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.IJob

object RecommendationEngineJob extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.RecommendationEngineJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.info("Started executing RecommendationEngineJob", className)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[MeasuredEvent]("batch", config, RecommendationEngine);
        JobLogger.info("RecommendationEngineJob completed....", className)
    }
}