package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.EOCRecommendationFunnelModel

/**
 * @author mahesh
 */

object EOCRecommendationFunnelSummarizer extends Application with IJob {
	implicit val className = "org.ekstep.analytics.job.summarizer.EOCRecommendationFunnelSummarizer"
	
	def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, EOCRecommendationFunnelModel);
    }
}
