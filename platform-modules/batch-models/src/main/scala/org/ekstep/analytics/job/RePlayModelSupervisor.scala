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
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.OutputDispatcher
import scala.sys.process._
object RePlayModelSupervisor extends Application {

    def main(model: String, fromDate: String, toDate: String, delta: Int, config: String) {

        val con = JSONUtils.deserialize[JobConfig](config)

        implicit val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));

        // patterns for start-date and end-date for yyyy-mm-dd formt 
        val matcher1Str = "(\\d{4}-\\d{2}-\\d{2})"
        val matcher2Str = "(endDate\":\"\\d{4}-\\d{2}-\\d{2})"
        val matcher3Str = "(startDate\":\"\\d{4}-\\d{2}-\\d{2})"
        val matcher1 = Pattern.compile(matcher1Str).matcher(config)
        val matcher2 = Pattern.compile(matcher2Str).matcher(config)
        val matcher3 = Pattern.compile(matcher3Str).matcher(config)

        model match {
            case "LearnerProficiencySummary" =>
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find || matcher3.find) {
                    var end = CommonUtil.getEndDate(Option(fromDate), delta)
                    for (date <- dateRange) {
                        println("processing for the date : " + date + "to" + end.get)
                        jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                        jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + date);
                        ProficiencyUpdater.main(jobConfig)(Option(sc));
                        end = CommonUtil.getEndDate(end, 1)
                    }
                } else {
                    ProficiencyUpdater.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "LearnerActivitySummary" =>
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find || matcher3.find) {
                    var end = CommonUtil.getEndDate(Option(fromDate), delta)
                    
                    //#### if date interval is less than the delta ####
                    if (!CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                        println("processing for the date : " + fromDate + "to" + toDate)
                        jobConfig = matcher2.replaceAll("endDate\":\"" + toDate);
                        jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + fromDate);
                        LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                    }
                    for (date <- dateRange) {
                        if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                            println("processing for the date : " + date + "to" + end.get)
                            jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                            jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + date);
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
                if (matcher2.find || matcher3.find) {
                    var end = CommonUtil.getEndDate(Option(fromDate), delta)

                    //#### if date interval is less than 7 days ####
                    if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                        jobConfig = matcher2.replaceAll("endDate\":\"" + toDate);
                        jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + fromDate);
                        LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                    }

                    for (date <- dateRange) {
                        if (CommonUtil.dateIsAfterOrEqual(toDate, end.get)) {
                            jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                            jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + date);
                            LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                            end = CommonUtil.getEndDate(end, 1)
                        }
                    }
                } else {
                    LearnerContentActivityUpdater.main(jobConfig)(Option(sc))
                }
                CommonUtil.closeSparkContext()(sc)
            case "RecommendationEngine" =>
                var jobConfig = config;
                val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
                if (matcher2.find || matcher3.find) {
                    var end = CommonUtil.getEndDate(Option(fromDate), delta)
                    for (date <- dateRange) {
                        println("processing for the date : " + date + "to" + end.get)
                        jobConfig = matcher2.replaceAll("endDate\":\"" + end.get);
                        jobConfig = Pattern.compile(matcher3Str).matcher(jobConfig).replaceAll("startDate\":\"" + date);
                        RecommendationEngineJob.main(jobConfig)(Option(sc));
                        end = CommonUtil.getEndDate(end, 1)
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