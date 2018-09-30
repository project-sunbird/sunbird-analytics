package org.ekstep.analytics.api.metrics

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.DeviceMetrics
import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.util._

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.config.Config
import com.weather.scalacass.syntax._

case class DeviceProfileTable(device_id: String, channel: String, first_access: Option[Long], last_access: Option[Long], total_ts: Option[Double], total_launches: Option[Long], avg_ts: Option[Double], spec: Option[Map[String, AnyRef]], updated_date: Long)

object DeviceMetricsModel extends IMetricsModel[DeviceMetrics, DeviceMetrics] with Serializable {

    override def metric: String = "ds";

    override def getMetrics(records: Array[DeviceMetrics], period: String = "CUMULATIVE", fields: Array[String] = Array())(implicit config: Config): Array[DeviceMetrics] = {
        val periodEnum = periodMap.get(period).get._1;
        val periods = _getPeriods(period);
        val recordsArray = records.map { x => (x.d_period.get, x) };
        val periodsArray = periods.map { period => (period, DeviceMetrics(Option(period), Option(CommonUtil.getPeriodLabel(periodEnum, period)))) };
        periodsArray.map(x => x._2)
    }
    override def getData(contentId: String, tags: Array[String], period: String, channel: String, userId: String = "all", deviceId: String = "all", metricsType: String = "app", mode: String = "")(implicit mf: Manifest[DeviceMetrics], config: Config): Array[DeviceMetrics] = {
        val query = QueryBuilder.select().all().from(Constants.DEVICE_DB, Constants.DEVICE_PROFILE_TABLE).allowFiltering().where(QueryBuilder.eq("device_id", deviceId)).and(QueryBuilder.eq("channel", channel)).toString()
        val res = DBUtil.session.execute(query)
        val metrics = getSummaryFromCass(res.one.as[DeviceProfileTable])
        return Array(metrics)
    }

    override def reduce(fact1: DeviceMetrics, fact2: DeviceMetrics, fields: Array[String] = Array()): DeviceMetrics = {
        return fact1
    }

    override def getSummary(summary: DeviceMetrics): DeviceMetrics = {
        return null
    }

    private def getSummaryFromCass(summary: DeviceProfileTable): DeviceMetrics = {
        DeviceMetrics(Option(0), None, summary.first_access, summary.last_access, summary.total_ts, summary.total_launches, summary.avg_ts, summary.spec)
    }
}