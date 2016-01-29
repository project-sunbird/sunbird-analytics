package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.LearnerProficiencySummary

object ProficiencyUpdater extends optional.Application {
    
    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, LearnerProficiencySummary);
    }
}