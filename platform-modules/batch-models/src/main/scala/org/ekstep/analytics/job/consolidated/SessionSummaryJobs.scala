package org.ekstep.analytics.job.consolidated

import org.apache.spark.SparkContext
import org.ekstep.analytics.model._
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.IJob

object SessionSummaryJobs extends optional.Application with IJob {

    val className = "org.ekstep.analytics.job.SessionSummaryJobs";
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val models = List(ContentUsageSummaryModel, ItemSummaryModel, StageSummaryModel)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, models, className);
    }
}