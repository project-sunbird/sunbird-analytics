package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.model.DerivedEventFieldExtractorModel
import org.ekstep.analytics.model.EventFieldExtractorModel

object CSVDumpJob extends optional.Application with IJob {
    
    implicit val className = "org.ekstep.analytics.job.CSVDumpJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        
        val eid = jobConfig.modelParams.getOrElse(Map()).getOrElse("eid", "ME_SESSION_SUMMARY").asInstanceOf[String];
        if(eid.startsWith("ME_")) {
            JobDriver.run("batch", config, DerivedEventFieldExtractorModel);    
        } else {
            JobDriver.run("batch", config, EventFieldExtractorModel);
        }
        
        JobLogger.log("Job Completed.")
    }
  
}