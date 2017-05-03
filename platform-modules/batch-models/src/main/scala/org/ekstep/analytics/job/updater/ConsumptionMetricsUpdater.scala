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
case class GenieStats(period: Int, content_usage_by_content_visits: Double, content_visits_by_genie_visits: Double, genie_visits_by_devices: Double, content_usage_by_device: Double)
case class MetricsComputation(period: Int, contentUsage: Double, contentVisits: Double, genieVisits: Double, devices: Double)
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
        val genieUsageSummary = getGenieUsageSummary(periodType, periods)
        val contentUsageSummary = getContentUsageSummary(periodType, periods)
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
        val genieTimespent = genieRdd.groupBy { x => x.d_period }.map { x => (x._1, x._2.map { x => x.m_total_ts }.sum) }
        val genieVisits = genieRdd.groupBy { x => x.d_period }.map { x => (x._1, x._2.map { x => x.m_total_sessions }.sum) }
        val contentUsage = contentRdd.groupBy { x => x.d_period }.map { x => (x._1, x._2.map { x => x.m_total_ts }.sum) }
        val contentVisits = contentRdd.groupBy { x => x.d_period }.map { x => (x._1, x._2.map { x => x.m_total_sessions }.sum.toDouble) }
        val devices = genieRdd.groupBy { x => x.d_period }.map { x => (x._1, x._2.map { x => x.m_total_devices }.sum.toDouble) }

        val metricsJoin = contentUsage.join(contentVisits).join(genieVisits).join(devices).map { case (period, (((contentUsage, contentVisits), genieVisits), devices)) => (period, contentUsage, contentVisits, genieVisits, devices) }
        val metricsComputation = metricsJoin.map { x => MetricsComputation(x._1, x._2, x._3, x._4, x._5) }
        val computation = metricsComputation.map { x =>
            GenieStats(x.period, x.contentUsage / x.contentVisits, x.contentVisits / x.genieVisits, x.genieVisits / x.devices, x.contentUsage / x.devices)
        }
        computation.map { x => Point(time = getDateTime(periodType, x.period.toString()), measurement = GENIE_STATS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase()), fields = Map("content_usage_by_content_visits" -> x.content_usage_by_content_visits, "content_visits_by_genie_visits" -> x.content_visits_by_genie_visits, "genie_visits_by_devices" -> x.genie_visits_by_devices, "content_usage_by_device" -> x.content_usage_by_device)) }
    }

    private def getGenieUsageSummary(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[GenieUsageSummaryFact] = {
        sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
    }

    private def getContentUsageSummary(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[ContentUsageSummaryFact] = {
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