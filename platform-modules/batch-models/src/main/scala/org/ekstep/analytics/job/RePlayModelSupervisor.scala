package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import java.util.regex.Pattern
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig

object RePlayModelSupervisor extends Application {

    def main(model: String, fromDate: String, toDate: String, config: String) {

        val con = JSONUtils.deserialize[JobConfig](config)
        
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
        // checking for yyyy-mm-dd formt in config 
        val matcher1 = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})").matcher(config)
        val matcher2 = Pattern.compile("(endDate\":\"\\d{4}-\\d{2}-\\d{2})").matcher(config)
        val matcher3 = Pattern.compile("(startDate\":\"\\d{4}-\\d{2}-\\d{2})").matcher(config)
        
        model match {
            case "LearnerProficiencySummary" =>
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher1.find) {
                    jobConfig = matcher1.replaceAll("__date__")
                    for (date <- dateRange) {
                        jobConfig = jobConfig.replace("__date__", date);
                        ProficiencyUpdater.main(jobConfig)(Option(sc))
                    }
                } else {
                    println("#### Executing Proficiency Summary from local file ####")
                    ProficiencyUpdater.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "LearnerActivitySummary" =>
                val con = JSONUtils.deserialize[JobConfig](config)
                val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find && matcher3.find) {
                    for (date <- dateRange) {
                        val end = CommonUtil.getEndDate(Option(date), 6)
                        if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                            jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                            jobConfig = matcher3.replaceAll("startDate\":\"" + date);
                            LearnerActivitySummarizer.main(jobConfig)(Option(sc))
                        }
                    }
                } else {
                    LearnerActivitySummarizer.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "LearnerContentActivitySummary" =>
                val con = JSONUtils.deserialize[JobConfig](config)
                val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find && matcher3.find) {
                    for (date <- dateRange) {
                        val end = CommonUtil.getEndDate(Option(date), 6)
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
                val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));
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