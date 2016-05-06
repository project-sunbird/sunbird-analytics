package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.model.LearnerProficiencySummary
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.JobFactory

object ReplaySupervisor extends Application {

    val className = "org.ekstep.analytics.job.ReplaySupervisor"

    def main(model: String, fromDate: String, toDate: String, config: String) {
        JobLogger.info("Started executing ReplaySupervisor", className)
        val con = JSONUtils.deserialize[JobConfig](config)
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
        try {
            execute(model, fromDate, toDate, config)(sc);
        } finally {
            CommonUtil.closeSparkContext()(sc);
        }
        JobLogger.info("Replay Supervisor completed...", className)
    }

    def execute(model: String, fromDate: String, toDate: String, config: String)(implicit sc: SparkContext) {
        val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
        for (date <- dateRange) {
            try {
                val jobConfig = config.replace("__endDate__", date)
                val job = JobFactory.getJob(model);
                job.main(jobConfig)(Option(sc));
            } catch {
                case ex: DataFetcherException => {
                    JobLogger.error("File is missing in S3 with date - " + date + " | Model - " + model, className, ex)
                    println("### File is missing in S3 with date - " + date + " | Model - " + model + " ###");
                }
                case ex: Exception => {
                    JobLogger.error("Unable to execute a Model with the code: " + model, className, ex)
                    throw ex;
                }
            }
        }
    }
}