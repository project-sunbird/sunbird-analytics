package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.ConceptSimilarityUpdater
import org.ekstep.analytics.updater.ConceptSimilarityEntity
import org.apache.spark.SparkContext

object ConceptSimilarityUpdaterJob extends optional.Application {

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run[ConceptSimilarityEntity]("batch", config, ConceptSimilarityUpdater);
    }
}