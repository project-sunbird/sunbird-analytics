package org.ekstep.analytics.job.consolidated

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.model._
import org.ekstep.analytics.framework.JobDriver

object SessionSummaryJobs extends Application {

    val className = "org.ekstep.analytics.job.SessionSummaryJobs";
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        val models = List(ContentUsageSummaryModel, ItemUsageSummaryModel, StageSummaryModel)
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, models, className);
    }
}