package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.ConceptSnapshotSummaryModel

object ConceptSnapshotSummarizer extends optional.Application with IJob {
  
    implicit val className = "org.ekstep.analytics.job.ConceptSnapshotSummarizer"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, ConceptSnapshotSummaryModel);
        JobLogger.log("Job Completed.")
    }
}