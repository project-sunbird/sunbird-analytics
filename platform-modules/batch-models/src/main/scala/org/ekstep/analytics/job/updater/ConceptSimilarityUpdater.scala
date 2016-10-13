package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.updater.UpdateConceptSimilarityDB

object ConceptSimilarityUpdater extends optional.Application {

    implicit val className = "org.ekstep.analytics.job.ConceptSimilarityUpdaterJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.log("Started executing Job")
        JobDriver.run("batch", config, UpdateConceptSimilarityDB);
        JobLogger.log("Job completed.")
    }
}