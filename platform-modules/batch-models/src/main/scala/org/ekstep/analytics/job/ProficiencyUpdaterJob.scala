package org.ekstep.analytics.job

import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.JobDriver
import optional.Application
import org.ekstep.analytics.model.ProficiencyUpdater

object ProficiencyUpdaterJob extends Application {
    
    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, ProficiencyUpdater);
    }
}