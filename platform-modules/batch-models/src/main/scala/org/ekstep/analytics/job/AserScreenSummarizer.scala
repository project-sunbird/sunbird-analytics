package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.AserScreenSummary
import org.ekstep.analytics.framework.Event
import optional.Application

/**
 * @author Santhosh
 */
object AserScreenSummarizer extends Application {
    
    def main(config: String) {
        JobDriver.run[Event]("batch", config, AserScreenSummary);
    }
  
}