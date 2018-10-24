package org.ekstep.analytics.api.metrics

import java.util.Date

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.DeviceMetrics
import org.ekstep.analytics.api.IMetricsModel
import org.ekstep.analytics.api.util.{APILogger, CommonUtil, DBUtil, JSONUtils}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.config.Config
import com.weather.scalacass.syntax._
import org.joda.time.DateTime

case class DeviceProfileTable(device_id: String, channel: String, first_access: Option[Long], last_access: Option[Long], total_ts: Option[Double], total_launches: Option[Long], avg_ts: Option[Double], spec: Option[Map[String, AnyRef]], updated_date: Long)

object DeviceMetricsModel extends IMetricsModel[DeviceMetrics, DeviceMetrics] with Serializable {

  override implicit val className = "org.ekstep.analytics.api.metrics.DeviceMetricsModel"

  override def metric: String = "ds"

  override def getMetrics(records: Array[DeviceMetrics], period: String, fields: Array[String] = Array())(implicit config: Config): Array[DeviceMetrics] = {
    records
  }

  override def getData(contentId: String, tags: Array[String], period: String, channel: String, userId: String = "all", deviceId: String = "all", metricsType: String = "app", mode: String = "")(implicit mf: Manifest[DeviceMetrics], config: Config): Array[DeviceMetrics] = {
    val query = QueryBuilder.select().all().from(Constants.DEVICE_DB, Constants.DEVICE_PROFILE_TABLE).allowFiltering().where(QueryBuilder.eq("device_id", deviceId)).and(QueryBuilder.eq("channel", channel)).toString()
    val res = DBUtil.session.execute(query).one
    val metrics = DeviceMetrics(Option(0), period, Option(res.as[Option[Date]]("first_access").get.getTime),
      Option(res.as[Option[Date]]("last_access").get.getTime), res.as[Option[Double]]("total_ts"),
      res.as[Option[Long]]("total_launches"), res.as[Option[Double]]("avg_ts"), res.as[Option[Map[String, String]]]("spec"))
    Array(metrics)
  }

  override def reduce(fact1: DeviceMetrics, fact2: DeviceMetrics, fields: Array[String] = Array()): DeviceMetrics = {
    fact1
  }

  override def getSummary(summary: DeviceMetrics): DeviceMetrics = {
    null
  }
}