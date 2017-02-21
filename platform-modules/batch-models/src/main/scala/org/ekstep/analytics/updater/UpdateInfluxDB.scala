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

case class genie_metrics(week: Int, d_tag: String, m_total_sessions: Double, m_total_ts: Double, time_stamp: Long)
case class conent_metrics(week: Int, d_tag: String, m_total_sessions: Double, m_total_ts: Double, time_stamp: Long)
case class metrics(genie_metrics: genie_metrics, conent_metrics: conent_metrics) extends Input with Output with AlgoInput with AlgoOutput
object UpdateInfluxDB extends IBatchModelTemplate[DerivedEvent, metrics, metrics, metrics] with Serializable {

    implicit val className = "org.ekstep.analytics.updater.UpdateInfluxDB"
    override def name: String = "UpdateInfluxDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[metrics] = {

        val genieUsageSummaryFact = sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).collect().toList
        val contentUsageSummaryFact = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).collect().toList
        val genieRdd = sc.parallelize(genieUsageSummaryFact)
        val contentRdd = sc.parallelize(contentUsageSummaryFact)
        val contentfilterRdd = contentRdd.filter { x => (x.d_period.toString().length() == 7) }
        val genieFilteredRdd = genieRdd.filter { x => (x.d_period.toString().length() == 7) }
        val content = contentfilterRdd.map { x => (x.d_period, (x.d_tag, x.m_total_sessions, x.m_total_ts)) }
        val genie = genieFilteredRdd.map { x => (x.d_period, (x.d_tag, x.m_total_sessions, x.m_total_ts)) }
        val join_genie_content = genie.join(content)
        val mapping = join_genie_content.map { case (a, ((b, c, d), (e, f, g))) => (a, b, c, d, e, f, g) }
        val week_to_timestamp = mapping.map { x =>
            val year = x._1.toString().split("7")(0)
            val week = x._1.toString().split("7")(1)
            (x, weekTomilliSeconds(year, week))
        }
        val map_timestamp = week_to_timestamp.map { case ((a, b, c, d, e, f, g), h) => (a, b, c, d, e, f, g, h) }
        map_timestamp.map { x => metrics(genie_metrics(x._1, x._2, x._3, x._4, x._8), conent_metrics(x._1, x._5, x._6, x._7, x._8)) }

    }

    override def algorithm(data: RDD[metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[metrics] = {
        import com.pygmalios.reactiveinflux.spark._
        // create database if not created
        checkdatabase
        //converting data to line protocol.........
        val genie = data.map { x => x.genie_metrics }.map { x => Point(time = new DateTime(x.time_stamp), measurement = "genie_metrics", tags = Map("d_tag" -> x.d_tag), fields = Map("m_total_sessions" -> x.m_total_sessions, "m_total_ts" -> x.m_total_ts, "week" -> x.week.toString())) }
        val content = data.map { x => x.conent_metrics }.map { x => Point(time = new DateTime(x.time_stamp), measurement = "content_metrics", tags = Map("d_tag" -> x.d_tag), fields = Map("m_total_sessions" -> x.m_total_sessions, "m_total_ts" -> x.m_total_ts, "week" -> x.week.toString())) }
        implicit val params = ReactiveInfluxDbName(Constants.INFLUX_DB_NAME)
        implicit val awaitAtMost = 1.second
        genie.saveToInflux()
        content.saveToInflux()
        data
    }

    override def postProcess(data: RDD[metrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[metrics] = {
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