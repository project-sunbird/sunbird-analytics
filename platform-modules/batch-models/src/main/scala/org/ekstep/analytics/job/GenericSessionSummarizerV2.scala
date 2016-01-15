package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.GenericSessionSummary
import org.ekstep.analytics.framework.Event
import optional.Application
import org.ekstep.analytics.model.GenericSessionSummaryV2

/**
 * @author Santhosh
 */
object GenericSessionSummarizerV2 extends Application {
    
    def main(config: String) {
        JobDriver.run[Event]("batch", config, GenericSessionSummaryV2);
    }
  
}