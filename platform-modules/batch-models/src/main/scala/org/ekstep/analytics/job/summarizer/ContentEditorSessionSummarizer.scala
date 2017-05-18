package org.ekstep.analytics.job.summarizer

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.LearnerSessionSummaryModel
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.ContentEditorSessionSummaryModel


/**
 * @author Jitendra Singh Sankhwar
 */
object ContentEditorSessionSummarizer extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ContentEditorSessionSummarizer"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, ContentEditorSessionSummaryModel);
        JobLogger.log("Job Completed.")
    }

}