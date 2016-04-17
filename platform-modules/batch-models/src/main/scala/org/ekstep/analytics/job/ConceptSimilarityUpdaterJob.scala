package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.ConceptSimilarityUpdater
import org.ekstep.analytics.updater.ConceptSimilarityEntity
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger

object ConceptSimilarityUpdaterJob extends optional.Application {

    val className = this.getClass.getName
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.info("Started executing ConceptSimilarityUpdaterJob", className)
        JobDriver.run[ConceptSimilarityEntity]("batch", config, ConceptSimilarityUpdater);
        JobLogger.info("ConceptSimilarityUpdaterJob completed .....", className)
    }
}