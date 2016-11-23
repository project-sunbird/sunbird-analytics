package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.GenieFunnelModel
import optional.Application
import org.ekstep.analytics.framework.IJob

object GenieFunnelSummarizer extends Application with IJob {

    implicit val className = "org.ekstep.analytics.job.summarizer.GenieFunnelModelSummarizer"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, GenieFunnelModel);

    }
}