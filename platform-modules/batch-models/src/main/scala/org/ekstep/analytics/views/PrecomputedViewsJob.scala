package org.ekstep.analytics.views

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
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.JobDriver

object PrecomputedViewsJob extends Application with IJob {

    implicit val className = "org.ekstep.analytics.job.PrecomputedViewsJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.start("Started executing PrecomputedViewsJob")
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobDriver.run("batch", config, PrecomputedViews);
        JobLogger.log("Job Completed.")

    }

}