/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.job.summarizer

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.MEUsageSummaryModel

object MEUsageSummarizer extends Application with IJob {

    implicit val className = "org.ekstep.analytics.job.MEUsageSummarizer"
  
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, MEUsageSummaryModel);
        JobLogger.log("Job Completed.")
    }
}