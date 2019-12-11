package org.ekstep.analytics.job

import org.ekstep.analytics.framework.FrameworkContext

/**
 * @author Santhosh
 */
object JobExecutor extends optional.Application {
    
    val className = "org.ekstep.analytics.job.ReplaySupervisor"

    def main(model: String, config: String)(implicit fc : FrameworkContext) {
        val job = JobFactory.getJob(model);
        job.main(config)(None, Option(fc));
    }
  
}