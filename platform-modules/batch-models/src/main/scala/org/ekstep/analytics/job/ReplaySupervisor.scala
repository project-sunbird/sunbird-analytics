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
import org.ekstep.analytics.framework.exception.JobNotFoundException
import org.ekstep.analytics.framework.Level._

object ReplaySupervisor extends Application {

    val className = "org.ekstep.analytics.job.ReplaySupervisor"

    def main(model: String, fromDate: String, toDate: String, config: String) {
        val t1 = System.currentTimeMillis;
        JobLogger.log("Started executing ReplaySupervisor", className, None, None, None, INFO)
        val con = JSONUtils.deserialize[JobConfig](config)
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
        try {
            execute(model, fromDate, toDate, config)(sc);
        } finally {
            CommonUtil.closeSparkContext()(sc);
        }
        val t2 = System.currentTimeMillis;
        JobLogger.log("Replay Supervisor completed.", className, None, Option(Map("start_date" -> fromDate, "end_date" -> toDate, "timeTaken" -> Double.box((t2 - t1) / 1000))), None, INFO)
    }

    def execute(model: String, fromDate: String, toDate: String, config: String)(implicit sc: SparkContext) {
        val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
        for (date <- dateRange) {
            try {
                val jobConfig = config.replace("__endDate__", date)
                val job = JobFactory.getJob(model);
                JobLogger.log("### Executing replay for the date - " + date + " ###", className, None, None, None, INFO)
                job.main(jobConfig)(Option(sc));
            } catch {
                case ex: DataFetcherException => {
                    JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("model_code" -> model, "date" -> date)), Option("FAILED"), ERROR)
                }
                case ex: JobNotFoundException => {
                    JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("model_code" -> model, "date" -> date)), Option("FAILED"), ERROR)
                    throw ex;
                }
                case ex: Exception => {
                    JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("model_code" -> model, "date" -> date)), Option("FAILED"), ERROR)
                    ex.printStackTrace()
                }
            }
        }
    }
}