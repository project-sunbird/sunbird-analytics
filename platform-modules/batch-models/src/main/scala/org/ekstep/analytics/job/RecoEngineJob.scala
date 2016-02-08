package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.model.RecoEngine
import org.ekstep.analytics.framework.JobDriver

object RecoEngineJob extends optional.Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, RecoEngine);
    }
}