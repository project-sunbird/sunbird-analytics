package org.ekstep.analytics.job

import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.model.LearnerProficiencySummary

object ReplaySupervisor extends Application {

    def main(model: String, fromDate: String, toDate: String, config: String) {

        val con = JSONUtils.deserialize[JobConfig](config)
        val sc = CommonUtil.getSparkContext(JobContext.parallelization, con.appName.getOrElse(con.model));

        val dateRange = CommonUtil.getDatesBetween(fromDate, Option(toDate))
        for (date <- dateRange) {
            val jobConfig = config.replace("__endDate__", date)
            model match {
                case "lp" =>
                    println("Running LearnerProficiencySummary for the date : " + date);
                    ProficiencyUpdater.main(jobConfig)(Option(sc));
                case "las" =>
                    println("Running LearnerActivitySummary for the date : " + date);
                    LearnerContentActivityUpdater.main(jobConfig)(Option(sc));
                case "lcas" =>
                    println("Running LearnerContentActivitySummary for the date : " + date);
                    LearnerContentActivityUpdater.main(jobConfig)(Option(sc));
                case "re" =>
                    println("Running RecommendationEngine for the date : " + date);
                    RecommendationEngineJob.main(jobConfig)(Option(sc));
                case _ =>
                    throw new Exception("Model Code is not correct");
            }
        }
        CommonUtil.closeSparkContext()(sc)
    }
}