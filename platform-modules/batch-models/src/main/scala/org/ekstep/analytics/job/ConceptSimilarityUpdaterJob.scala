package org.ekstep.analytics.job

import org.ekstep.analytics.framework.ConceptSimilarityJson
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.updater.ConceptSimilarityUpdater

object ConceptSimilarityUpdaterJob extends Application {

    def main(config: String) {
        JobDriver.run[ConceptSimilarityJson]("batch", config, ConceptSimilarityUpdater);
    }
}