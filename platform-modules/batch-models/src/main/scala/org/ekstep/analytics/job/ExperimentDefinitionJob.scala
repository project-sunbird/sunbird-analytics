package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{IJob, JobDriver}
import org.ekstep.analytics.model.ExperimentDefinitionModel

object ExperimentDefinitionJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ExperimentDefinitionJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, ExperimentDefinitionModel);
        JobLogger.log("Job Completed.")

    }
}