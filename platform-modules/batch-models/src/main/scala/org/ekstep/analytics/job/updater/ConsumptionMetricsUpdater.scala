package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.updater.GenieUsageSummaryFact
import org.ekstep.analytics.util.ContentUsageSummaryFact
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._
import com.pygmalios.reactiveinflux._
import com.datastax.spark.connector._

/**
 * @author mahesh
 */

object ConsumptionMetricsUpdater extends Application with IJob {

    implicit val className = "org.ekstep.analytics.job.ConsumptionMetricsUpdater"
    val GENIE_METRICS = "genie_metrics";
    val CONTENT_METRICS = "content_metrics";
    val GENIE_STATS = "genie_stats"
    val ALL_PERIOD_TYPES = List("MONTH", "WEEK", "DAY");

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.init("ConsumptionMetricsUpdater")
        JobLogger.start("ConsumptionMetricsUpdater Job Started executing", Option(Map("config" -> config)))
        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            try {
                execute(jobConfig);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(jobConfig);
        }
    }

    private def execute(config: JobConfig)(implicit sc: SparkContext) = {
        val modelParams = config.modelParams.get;
        val periodType = modelParams.getOrElse("periodType", WEEK.toString()).asInstanceOf[String];
        val periodUpTo = modelParams.getOrElse("periodUpTo", 100).asInstanceOf[Int];
        val metrics = if ("ALL".equals(periodType)) {
            ALL_PERIOD_TYPES.map { period => getMetrics(period, periodUpTo) }.reduce((a, b) => a ++ b);
        } else {
            getMetrics(periodType, periodUpTo);
        }
        import com.pygmalios.reactiveinflux.spark._
        implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
        metrics.saveToInflux()
        JobLogger.end("ConsumptionMetricsUpdater Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> metrics.count(), "outputEvents" -> metrics.count(), "timeTaken" -> 0)));
    }

    private def getMetrics(periodType: String, periodUpTo: Int)(implicit sc: SparkContext): RDD[Point] = {
        val periods = CommonUtil.getPeriods(periodType, periodUpTo).toList;
        val genieUsageSummary = getGenieFromCassandra(periodType, periods)
        val contentUsageSummary = getContentFromCassandra(periodType, periods)
        val genieMetrics = getGenieMetrics(periodType, genieUsageSummary)
        val contentMetrics = getContentMetrics(periodType, contentUsageSummary)
        val genieStats = getGenieStats(periodType, genieUsageSummary, contentUsageSummary)
        genieMetrics ++ contentMetrics ++ genieStats
    }

    private def getGenieMetrics(periodType: String, genieRdd: RDD[GenieUsageSummaryFact])(implicit sc: SparkContext): RDD[Point] = {
        genieRdd.map { x => Point(time = getDateTime(periodType, x.d_period.toString), measurement = GENIE_METRICS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase(), "tag" -> x.d_tag), fields = Map("sessions" -> x.m_total_sessions.toDouble, "timespent" -> x.m_total_ts)) };
    }

    private def getContentMetrics(periodType: String, contentRdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[Point] = {
        contentRdd.map { x => Point(time = getDateTime(periodType, x.d_period.toString), measurement = CONTENT_METRICS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase(), "tag" -> x.d_tag, "content" -> x.d_content_id), fields = Map("sessions" -> x.m_total_sessions.toDouble, "timespent" -> x.m_total_ts)) };
    }

    private def getGenieStats(periodType: String, genieRdd: RDD[GenieUsageSummaryFact], contentRdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): RDD[Point] = {
        val genieTimeSpent = genieRdd.map { x => x.m_total_ts }.reduce((a, b) => a + b)
        val genieVisits = genieRdd.map { x => x.m_total_sessions }.reduce((a, b) => a + b)
        val contentUsage = contentRdd.map { x => x.m_total_ts }.reduce((a, b) => a + b)
        val contentVisits = contentRdd.map { x => x.m_total_sessions }.reduce((a, b) => a + b)
        val period = contentRdd.first().d_period.toString()
        val devices = genieRdd.map { x => x.m_total_devices }.reduce((a, b) => a + b)

        val content_usage_By_content_visits = contentUsage / contentVisits
        val content_visits_By_genie_visits = contentVisits / genieVisits
        val genie_visits_By_devices = genieVisits / devices
        val content_usage_By_device = contentUsage / devices

        val point = Point(time = getDateTime(periodType, period), measurement = GENIE_STATS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase()), fields = Map("content_usage_By_content_visits" -> content_usage_By_content_visits, "content_visits_By_genie_visits" -> content_visits_By_genie_visits.toDouble, "genie_visits_By_devices" -> genie_visits_By_devices.toDouble, "content_usage_By_device" -> content_usage_By_device))
        sc.parallelize(List(point))
    }

    private def getGenieFromCassandra(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[GenieUsageSummaryFact] = {
        sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
    }

    private def getContentFromCassandra(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[ContentUsageSummaryFact] = {
        sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
    }

    private def getDateTime(periodType: String, period: String): DateTime = {
        periodType match {
            case "DAY" => dayPeriod.parseDateTime(period).withTimeAtStartOfDay();
            case "WEEK" =>
                val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
                val firstDay = weekPeriodLabel.parseDateTime(week)
                val lastDay = firstDay.plusDays(6);
                lastDay.withTimeAtStartOfDay();
            case "MONTH" => monthPeriod.parseDateTime(period).withTimeAtStartOfDay();
        }
    }
}