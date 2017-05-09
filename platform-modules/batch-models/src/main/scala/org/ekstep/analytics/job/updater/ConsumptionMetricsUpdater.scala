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
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.framework.Level._
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
        val metrics = CommonUtil.time({
            val modelParams = config.modelParams.get;
            val periodType = modelParams.getOrElse("periodType", WEEK.toString()).asInstanceOf[String];
            val periodUpTo = modelParams.getOrElse("periodUpTo", 100).asInstanceOf[Int];
            val metrics = if ("ALL".equals(periodType)) {
                ALL_PERIOD_TYPES.map { period => getMetrics(period, periodUpTo) }.reduce((a, b) => a + b);
            } else {
                getMetrics(periodType, periodUpTo);
            }
            metrics
        })
        JobLogger.end("ConsumptionMetricsUpdater Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> metrics._2, "outputEvents" -> metrics._2, "timeTaken" -> metrics._1)));
    }

    private def getMetrics(periodType: String, periodUpTo: Int)(implicit sc: SparkContext): Long = {
        val periods = CommonUtil.getPeriods(periodType, periodUpTo).toList;
        val genieUsageSummary = getGenieUsageSummary(periodType, periods)
        val contentUsageSummary = getContentUsageSummary(periodType, periods)
        val genieMetrics = getGenieMetrics(periodType, genieUsageSummary)
        val contentMetrics = getContentMetrics(periodType, contentUsageSummary)
        val genieStats = getGenieStats(periodType, genieUsageSummary, contentUsageSummary)
        JobLogger.log("Metrics count", Option(Map("date" -> "", "periodType" -> periodType, "periodUpTo" -> periodUpTo, "genieMetrics" -> genieMetrics, "contentMetrics" -> contentMetrics, "genieStats" -> genieStats)), INFO);
        genieMetrics + contentMetrics + genieStats
    }

    private def getGenieMetrics(periodType: String, genieRdd: RDD[GenieUsageSummaryFact])(implicit sc: SparkContext): Long = {
        val metrics = genieRdd.map { x =>
            val fields = Map("sessions" -> x.m_total_sessions.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef], "timespent" -> x.m_total_ts.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef])
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "tag" -> x.d_tag), fields, time._1);
        };
        InfluxDBDispatcher.dispatch(GENIE_METRICS, metrics);
        metrics.count()
    }

    private def getContentMetrics(periodType: String, contentRdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): Long = {

        val metrics = contentRdd.map { x =>
            val fields = Map("sessions" -> x.m_total_sessions.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef], "timespent" -> x.m_total_ts.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef])
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "tag" -> x.d_tag, "content" -> x.d_content_id), fields, time._1);
        };
        InfluxDBDispatcher.dispatch(CONTENT_METRICS, metrics);
        metrics.count()
    }

    private def getGenieStats(periodType: String, genieRdd: RDD[GenieUsageSummaryFact], contentRdd: RDD[ContentUsageSummaryFact])(implicit sc: SparkContext): Long = {
        val genieTimespent = genieRdd.map { x => (x.d_period, x.m_total_ts) };
        val genieVisits = genieRdd.map { x => (x.d_period, x.m_total_sessions) };
        val contentUsage = contentRdd.map { x => (x.d_period, x.m_total_ts) }
        val contentVisits = contentRdd.map { x => (x.d_period, x.m_total_sessions.toDouble) };
        val devices = genieRdd.map { x => (x.d_period, x.m_total_devices.toDouble) };
        val metricsJoin = contentUsage.join(contentVisits).join(genieVisits).join(devices).map { case (period, (((contentUsage, contentVisits), genieVisits), devices)) => (period, contentUsage, contentVisits, genieVisits, devices) }
        val metricsComputation = metricsJoin.map { x => MetricsComputation(x._1, x._2, x._3, x._4, x._5) }
        val computation = metricsComputation.map { x =>
            val contentUsageByVisits = if (0 == x.contentVisits) 0 else x.contentUsage / x.contentVisits;
            val contentVisitsByGenieVisits = if (0 == x.genieVisits) 0 else x.contentVisits / x.genieVisits;
            val genieVisitsByDevice = if (0 == x.devices) 0 else x.genieVisits / x.devices;
            val contentUsageByDevice = if (0 == x.devices) 0 else x.contentUsage / x.devices;
            GenieStats(x.period, contentUsageByVisits, contentVisitsByGenieVisits, genieVisitsByDevice, contentUsageByDevice);
        }

        val metrics = computation.map { x =>
            val fields = Map("content_usage_by_content_visits" -> x.content_usage_by_content_visits.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef], "content_visits_by_genie_visits" -> x.content_visits_by_genie_visits.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef], "genie_visits_by_devices" -> x.genie_visits_by_devices.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef], "content_usage_by_device" -> x.content_usage_by_device.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef])
            val time = getDateTime(x.period);
            InfluxRecord(Map("period" -> time._2), fields, time._1);
        };
        InfluxDBDispatcher.dispatch(GENIE_STATS, metrics);
        metrics.count()
    }

    private def getGenieUsageSummary(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[GenieUsageSummaryFact] = {
        sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
    }

    private def getContentUsageSummary(periodType: String, periods: List[Int])(implicit sc: SparkContext): RDD[ContentUsageSummaryFact] = {
        sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
    }

    private def getDateTime(periodVal: Int): (DateTime, String) = {
        val period = periodVal.toString();
        period.size match {
            case 8 => (dayPeriod.parseDateTime(period).withTimeAtStartOfDay(), "day");
            case 7 =>
                val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
                val firstDay = weekPeriodLabel.parseDateTime(week)
                val lastDay = firstDay.plusDays(6);
                (lastDay.withTimeAtStartOfDay(), "week");
            case 6 => (monthPeriod.parseDateTime(period).withTimeAtStartOfDay(), "month");
        }
    }
}