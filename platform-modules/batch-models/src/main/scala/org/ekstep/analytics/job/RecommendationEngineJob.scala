package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.RecommendationEngine
import org.ekstep.analytics.framework.JobDriver

object RecommendationEngineJob extends optional.Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, RecommendationEngine);
    }
}