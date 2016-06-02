package org.ekstep.analytics.job

/**
 * @author Santhosh
 */
object JobExecutor extends optional.Application {
    
    val className = "org.ekstep.analytics.job.ReplaySupervisor"

    def main(model: String, config: String) {
        val job = JobFactory.getJob(model);
        job.main(config);
    }
  
}