package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import java.util.regex.Pattern
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.driver.BatchJobDriver
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.model.LearnerProficiencySummary
import org.ekstep.analytics.framework.JobDriver

object RePlayModelSupervisor extends Application {

    def main(model: String, fromDate: String, toDate: String, config: String) {
        
        val con = JSONUtils.deserialize[JobConfig](config)
        implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));

        // patterns for start-date and end-date for yyyy-mm-dd formt 
        val matcher1 = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})").matcher(config)
        val matcher2 = Pattern.compile("(endDate\":\"\\d{4}-\\d{2}-\\d{2})").matcher(config)
        val matcher3 = Pattern.compile("(startDate\":\"\\d{4}-\\d{2}-\\d{2})").matcher(config)

        model match {
            case "LearnerProficiencySummary" =>
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher1.find) {
                    for (date <- dateRange) {
                        try {
                            println("Running for date: ", date)
                            jobConfig = matcher1.replaceAll(date);
                            //ProficiencyUpdater.main(jobConfig)(Option(sc))
                            JobDriver.run("batch", jobConfig, LearnerProficiencySummary);
                        } catch {
                            case e: Exception => println("Got an empty result: " + e);
                        }
                    }
                } else {
                    println("#### Executing Proficiency Summary from local file ####")
                    ProficiencyUpdater.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "LearnerActivitySummary" =>
                val con = JSONUtils.deserialize[JobConfig](config)
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find && matcher3.find) {
                    var end = CommonUtil.getEndDate(Option(fromDate), 6)
                    for (date <- dateRange) {
                        if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                            jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                            jobConfig = matcher3.replaceAll("startDate\":\"" + date);
                            LearnerActivitySummarizer.main(jobConfig)(Option(sc))
                            end = CommonUtil.getEndDate(end, 1)
                        }
                    }
                } else {
                    LearnerActivitySummarizer.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "LearnerContentActivitySummary" =>
                val con = JSONUtils.deserialize[JobConfig](config)
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find && matcher3.find) {

                    val end = CommonUtil.getEndDate(Option(fromDate), 6)
                    for (date <- dateRange) {

                        if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                            jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                            jobConfig = matcher3.replaceAll("startDate\":\"" + date);
                            LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                        }
                    }
                } else {
                    LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "RecommendationEngine" =>
                val con = JSONUtils.deserialize[JobConfig](config)
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher1.find) {
                    jobConfig = matcher1.replaceAll("__date__")
                    for (date <- dateRange) {
                        jobConfig = jobConfig.replace("__date__", date);
                        RecommendationEngineJob.main(jobConfig)(Option(sc))
                    }
                } else {
                    RecommendationEngineJob.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case _ =>
                throw new Exception("Model name is not correct");
        }
    }
}