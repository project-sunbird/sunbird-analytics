package org.ekstep.analytics.job.updater

import com.datastax.spark.connector._
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
import org.apache.spark.rdd.RDD
import com.pygmalios.reactiveinflux._
import scala.concurrent.duration._
import org.ekstep.analytics.updater.GenieUsageSummaryFact
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

/**
 * @author mahesh
 */

object ConsumptionMetricsUpdater extends Application with IJob {

	implicit val className = "org.ekstep.analytics.job.ConsumptionMetricsUpdater"
	val GENIE_METRICS = "genie_metrics";
	val CONTENT_METRICS = "content_metrics";
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
			ALL_PERIOD_TYPES.map { period => getGenieMetrics(period, periodUpTo) }.reduce((a,b) => a ++ b);
		} else {
			getGenieMetrics(periodType, periodUpTo);
		}
		import com.pygmalios.reactiveinflux.spark._
		implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
        metrics.saveToInflux()
        JobLogger.end("ConsumptionMetricsUpdater Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> metrics.count(), "outputEvents" -> metrics.count(), "timeTaken" -> 0)));
	}

	private def getGenieMetrics(periodType: String, periodUpTo: Int)(implicit sc: SparkContext): RDD[Point] = {
		val periods = CommonUtil.getPeriods(periodType, periodUpTo).toList;
		val genieRdd = sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period IN?", periods).map { x => x };
		val metrics = genieRdd.map { x => Point(time = getDateTime(periodType, x.d_period.toString), measurement = GENIE_METRICS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase(), "tag" -> x.d_tag), fields = Map("sessions" -> x.m_total_sessions.toDouble, "timespent" -> x.m_total_ts)) };
		metrics
	}

	private def getContentMetrics(periods: List[Int])(implicit sc: SparkContext): RDD[Point] = {
		sc.emptyRDD[Point];
	}

	private def getDateTime(periodType: String, period: String): DateTime = {
		periodType match {
    		case "DAY"	=> dayPeriod.parseDateTime(period).withTimeAtStartOfDay();
    		case "WEEK"	=> 
    			val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
            	val firstDay = weekPeriodLabel.parseDateTime(week)
            	val lastDay = firstDay.plusDays(6);
            	lastDay.withTimeAtStartOfDay();
    		case "MONTH"      => monthPeriod.parseDateTime(period).withTimeAtStartOfDay();
    	}
	}
}