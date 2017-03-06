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
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._
import com.pygmalios.reactiveinflux._
import com.datastax.spark.connector._
/**
 * @author mahesh
 */
case class Items(period: String, template_id: String, number_of_Items: Long)
object CreationMetricsUpdater extends Application with IJob {
    val ITEM_METRICS = "item_metrics";
    val ALL_PERIOD_TYPES = List("MONTH", "WEEK", "DAY");

    implicit val className = "org.ekstep.analytics.job.updater.CreationMetricsUpdater"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        JobLogger.init("CreationMetricsUpdater")
        JobLogger.start("CreationMetricsUpdater Job Started executing", Option(Map("config" -> config)))
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
            ALL_PERIOD_TYPES.map { period => getItems(config, period, periodUpTo) }.reduce((a, b) => a ++ b);
        } else {
            getItems(config, periodType, periodUpTo);
        }
        import com.pygmalios.reactiveinflux.spark._
        implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
        metrics.saveToInflux()
        JobLogger.end("CreationMetricsUpdater Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> metrics.count(), "outputEvents" -> metrics.count(), "timeTaken" -> 0)));

    }
    private def getItems(config: JobConfig, periodType: String, periodUpTo: Int)(implicit sc: SparkContext): RDD[Point] = {
        val periods = CommonUtil.getPeriods(periodType, periodUpTo).toList;
        val fetcher = config.search
        val items = DataFetcher.fetchBatchData[Items](fetcher)
        val filtereditems = items.filter { x => periods.contains(x.period.toInt) }
        filtereditems.map { x => Point(time = getDateTime(periodType, x.period.toString), measurement = ITEM_METRICS, tags = Map("env" -> AppConf.getConfig("application.env"), "period" -> periodType.toLowerCase(), "template_id" -> x.template_id), fields = Map("number_of_Items" -> x.number_of_Items.toDouble)) };
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