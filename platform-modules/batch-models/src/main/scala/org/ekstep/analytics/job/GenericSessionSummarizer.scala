package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.GenericSessionSummary
import org.ekstep.analytics.framework.Event
import optional.Application

/**
 * @author Santhosh
 */
object GenericSessionSummarizer extends Application {
    
    def main(config: String) {
        JobDriver.run[Event]("batch", config, GenericSessionSummary);
    }
  
}