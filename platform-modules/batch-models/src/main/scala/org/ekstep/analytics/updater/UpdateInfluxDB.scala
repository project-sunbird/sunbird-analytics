package org.ekstep.analytics.updater
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import com.pygmalios.reactiveinflux._
import com.pygmalios.reactiveinflux.spark._
import scala.concurrent.duration._
import java.net.URI
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.util.ContentUsageSummaryFact
import java.util.Calendar

case class Genie_metrics(week: Int, d_tag: String, m_total_sessions: Double, m_total_ts: Double, time_stamp: Long)
case class Conent_metrics(week: Int, d_tag: String, m_total_sessions: Double, m_total_ts: Double, time_stamp: Long)
case class Items(period: Int, template_id: String, number_of_Items: Double, time_stamp: Option[Long] = None) extends Input
case class Metrics(genie_metrics: Genie_metrics, conent_metrics: Conent_metrics, items: Items) extends Input with Output with AlgoInput with AlgoOutput

object UpdateInfluxDB extends IBatchModelTemplate[Items, Metrics, Metrics, Metrics] with Serializable {
    implicit val className = "org.ekstep.analytics.updater.UpdateInfluxDB"
    override def name: String = "UpdateInfluxDB"

    override def preProcess(data: RDD[Items], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
        val genieUsageSummaryFact = sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).collect().toList
        val contentUsageSummaryFact = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).collect().toList
        val genieRdd = sc.parallelize(genieUsageSummaryFact)
        val contentRdd = sc.parallelize(contentUsageSummaryFact)
        val maping = data.map { x => (x.period, (x.template_id, x.number_of_Items)) }
        val contentFilterRdd = contentRdd.filter { x => (x.d_period.toString().length() == 7) }
        val genieFilterRdd = genieRdd.filter { x => (x.d_period.toString().length() == 7) }
        val content = contentFilterRdd.map { x => ((x.d_period, x.d_tag), (x.m_total_sessions, x.m_total_ts)) }
        val genie = genieFilterRdd.map { x => ((x.d_period, x.d_tag), (x.m_total_sessions, x.m_total_ts)) }
        val joinGenieContent = genie.join(content)
        val weekToTimestamp = joinGenieContent.map { x =>
            val year = x._1._1.toString().split("7")(0)
            val week = x._1._1.toString().split("7")(1)
            (x, weekTomilliSeconds(year, week))
        }
        val mapTimestamp = weekToTimestamp.map { case (((a, b), ((c, d), (e, f))), g) => (a, (b, c, d, e, f, g)) }
        val mapItems = mapTimestamp.join(maping).map { case (a, ((b, c, d, e, f, g), (h, i))) => (a, b, c, d, e, f, g, h, i) }
        mapItems.map { x => Metrics(Genie_metrics(x._1, x._2, x._3, x._4, x._7), Conent_metrics(x._1, x._2, x._5, x._6, x._7), Items(x._1, x._8, x._9, Some(x._7))) }
    }

    override def algorithm(data: RDD[Metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
        import com.pygmalios.reactiveinflux.spark._
        // create database if not created
        checkdatabase
        //converting data to line protocol.........
        val genie = data.map { x => x.genie_metrics }.map { x => Point(time = new DateTime(x.time_stamp), measurement = "genie_metrics", tags = Map("d_tag" -> x.d_tag, "week" -> x.week.toString()), fields = Map("m_total_sessions" -> x.m_total_sessions, "m_total_ts" -> x.m_total_ts)) }
        val content = data.map { x => x.conent_metrics }.map { x => Point(time = new DateTime(x.time_stamp), measurement = "content_metrics", tags = Map("d_tag" -> x.d_tag, "week" -> x.week.toString()), fields = Map("m_total_sessions" -> x.m_total_sessions, "m_total_ts" -> x.m_total_ts)) }
        val items = data.map { x => x.items }.map { x => Point(time = new DateTime(x.time_stamp.get), measurement = "item_metrics", tags = Map("template_id" -> x.template_id, "week" -> x.period.toString()), fields = Map("number_of_items" -> x.number_of_Items)) }
        implicit val params = ReactiveInfluxDbName(Constants.INFLUX_DB_NAME)
        implicit val awaitAtMost = 1.second
        genie.saveToInflux()
        content.saveToInflux()
        items.saveToInflux()
        data
    }

    override def postProcess(data: RDD[Metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Metrics] = {
        data
    }

    def weekTomilliSeconds(year: String, week: String): Long = {
        val cld = Calendar.getInstance();
        cld.set(Calendar.YEAR, year.toInt);
        cld.set(Calendar.WEEK_OF_YEAR, week.toInt);
        cld.getTimeInMillis;
    }

    def checkdatabase = {
        import com.paulgoldbaum.influxdbclient._
        import scala.concurrent.ExecutionContext.Implicits.global
        val influxdb = InfluxDB.connect(Constants.LOCAL_HOST, Constants.INFLUX_DB_PORT)
        val database = influxdb.selectDatabase(Constants.INFLUX_DB_NAME)
        if (database.exists() != true) {
            database.create()
        }
    }

}