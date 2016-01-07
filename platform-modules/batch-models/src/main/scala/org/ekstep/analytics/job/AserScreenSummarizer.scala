package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.AserScreenSummary
import org.ekstep.analytics.framework.Event

/**
 * @author Santhosh
 */
object AserScreenSummarizer extends Application {
    
    def main(config: String) {
        JobDriver.run[Event]("batch", config, AserScreenSummary);
    }
  
}