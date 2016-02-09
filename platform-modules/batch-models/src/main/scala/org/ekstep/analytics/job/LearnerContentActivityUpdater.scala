package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.updater.LearnerContentActivitySummary

object LearnerContentActivityUpdater extends optional.Application {

    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, LearnerContentActivitySummary);
    }
}