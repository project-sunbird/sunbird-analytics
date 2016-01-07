package org.ekstep.analytics.job

import org.ekstep.analytics.framework.util.Application
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.model.AserScreenSummary

/**
 * @author Santhosh
 */
object AserScreenSummarizer extends Application {
    
    def main(config: String) {
        JobDriver.run("batch", config, AserScreenSummary);
    }
  
}