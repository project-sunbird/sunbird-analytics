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

object ReplaySupervisor extends Application {

    val className = this.getClass.getName

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
                model.toLowerCase() match {
                    case "ss" =>
                        println("Running LearnerSessionSummary for the date : " + date);
                        LearnerSessionSummarizer.main(jobConfig)(Option(sc));
                    case "ssv2" =>
                        println("Running LearnerSessionSummaryV2 for the date : " + date);
                        LearnerSessionSummarizerV2.main(jobConfig)(Option(sc));
                    case "as" =>
                        println("Running AserScreenSummary for the date : " + date);
                        AserScreenSummarizer.main(jobConfig)(Option(sc));
                    case "lp" =>
                        println("Running LearnerProficiencySummary for the date : " + date);
                        ProficiencyUpdater.main(jobConfig)(Option(sc));
                    case "las" =>
                        println("Running LearnerActivitySummary for the date : " + date);
                        LearnerActivitySummarizer.main(jobConfig)(Option(sc));
                    case "ls" =>
                        println("Running LearnerSnapshot for the date : " + date);
                        LearnerSnapshotUpdater.main(jobConfig)(Option(sc));
                    case "lcas" =>
                        println("Running LearnerContentActivitySummary for the date : " + date);
                        LearnerContentActivityUpdater.main(jobConfig)(Option(sc));
                    case "lcr" =>
                        println("Running RecommendationEngine for the date : " + date);
                        RecommendationEngineJob.main(jobConfig)(Option(sc));
                    case _ =>
                        throw new Exception("Model Code is not correct");
                }
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