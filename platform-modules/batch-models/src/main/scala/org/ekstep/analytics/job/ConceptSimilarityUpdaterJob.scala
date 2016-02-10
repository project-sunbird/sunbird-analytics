package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.ConceptSimilarityUpdater
import org.ekstep.analytics.updater.ConceptSimilarityEntity

object ConceptSimilarityUpdaterJob extends Application {

    def main(config: String) {
        JobDriver.run[ConceptSimilarityEntity]("batch", config, ConceptSimilarityUpdater);
    }
}