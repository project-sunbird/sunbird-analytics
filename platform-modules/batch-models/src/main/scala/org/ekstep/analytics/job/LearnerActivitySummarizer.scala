package org.ekstep.analytics.job

import optional.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.LearnerActivitySummary
import org.ekstep.analytics.framework.MeasuredEvent

/**
 * @author Santhosh
 */
object LearnerActivitySummarizer extends Application {
    
    def main(config: String) {
        JobDriver.run[MeasuredEvent]("batch", config, LearnerActivitySummary);
    }
  
}