package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework.Level._
import org.ekstep.analytics.views.PrecomputedViews

object PrecomputedViewsJob extends Application {
	
	implicit val className = "org.ekstep.analytics.job.PrecomputedViewsJob"
	
	def main() {
		JobLogger.start("Started executing PrecomputedViews Job")
		val sc = CommonUtil.getSparkContext(JobContext.parallelization, className);
		try {
			PrecomputedViews.execute()(sc);
			JobLogger.end("PrecomputedViews Job Completed.", "SUCCESS", None);
		} catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, None, ERROR);
                JobLogger.end("Precomputed Views Job failed", "FAILED")
                throw ex
        } finally {
            CommonUtil.closeSparkContext()(sc);
        }
        
	}
  
}