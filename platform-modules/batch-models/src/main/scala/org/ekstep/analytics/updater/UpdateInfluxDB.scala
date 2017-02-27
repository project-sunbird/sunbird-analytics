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
import scala.util.matching.Regex
import org.ekstep.analytics.framework.util.CommonUtil
import scala.concurrent.{ Await, Future }

case class Items(period: Int, template_id: String, number_of_Items: Double) extends Input with AlgoInput with AlgoOutput with Output
object UpdateInfluxDB extends IBatchModelTemplate[Items, Items, Items, Items] with Serializable {
    implicit val className = "org.ekstep.analytics.updater.UpdateInfluxDB"
    override def name: String = "UpdateInfluxDB"

    override def preProcess(data: RDD[Items], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Items] = {
        val periods = org.ekstep.analytics.framework.util.CommonUtil.getPeriods("WEEK", Constants.WEEK_PERIODS.toInt).toList
        val genieUsageSummaryFact = sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period IN?", periods).collect().toList
        val contentUsageSummaryFact = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_period IN?", periods).collect().toList
        val genieRdd = sc.parallelize(genieUsageSummaryFact)
        val contentRdd = sc.parallelize(contentUsageSummaryFact)
        val filtereditems = data.filter { x => periods.contains(x.period) }
        import com.pygmalios.reactiveinflux.spark._
        // create database if not created
        checkdatabase(Constants.ALTER_RETENTION_POLICY)
        //converting data to line protocol.........
        val genie1 = genieRdd.map { x => Point(time = new DateTime(weekTomilliSeconds(x.d_period.toString())), measurement = "genie_metrics", tags = Map("d_tag" -> x.d_tag, "week" -> x.d_period.toString()), fields = Map("m_total_sessions" -> x.m_total_sessions.toDouble, "m_total_ts" -> x.m_total_ts)) }
        val content1 = contentRdd.map { x => Point(time = new DateTime(weekTomilliSeconds(x.d_period.toString())), measurement = "content_metrics", tags = Map("d_tag" -> x.d_tag, "week" -> x.d_period.toString()), fields = Map("m_total_sessions" -> x.m_total_sessions.toDouble, "m_total_ts" -> x.m_total_ts)) }
        val items1 = data.map { x => Point(time = new DateTime(weekTomilliSeconds(x.period.toString())), measurement = "item_metrics", tags = Map("template_id" -> x.template_id, "week" -> x.period.toString()), fields = Map("number_of_items" -> x.number_of_Items)) }
        implicit val params = ReactiveInfluxDbName(Constants.INFLUX_DB_NAME)
        implicit val awaitAtMost = 10.second
        genie1.saveToInflux()
        content1.saveToInflux()
        items1.saveToInflux()
        data
    }

    override def algorithm(data: RDD[Items], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Items] = {
        data
    }

    override def postProcess(data: RDD[Items], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Items] = {
        data
    }

    def weekTomilliSeconds(period: String): Long = {
        val year = period.substring(0, 4)
        val week = period.substring(5, 7)
        val cld = Calendar.getInstance();
        cld.set(Calendar.YEAR, year.toInt);
        cld.set(Calendar.WEEK_OF_YEAR, week.toInt);
        cld.set(Calendar.DAY_OF_WEEK, 8)
        cld.getTimeInMillis;
    }

    def checkdatabase(change: String) = {
        import com.paulgoldbaum.influxdbclient._
        import scala.concurrent.ExecutionContext.Implicits.global
        val influxdb = InfluxDB.connect(Constants.LOCAL_HOST, Constants.INFLUX_DB_PORT.toInt)
        val database = influxdb.selectDatabase(Constants.INFLUX_DB_NAME)
        val res = Await.result(database.exists(), 10 seconds)
        if (res != true) {
            database.create()
            database.createRetentionPolicy(Constants.RETENTION_POLICY_NAME, Constants.RETENTION_POLICY_DURATION, Constants.RETENTION_POLICY_REPLICATION.toInt, true)
        }
        if ((res == true) && (change == "YES")) {
            database.alterRetentionPolicy(Constants.RETENTION_POLICY_NAME, Constants.RETENTION_POLICY_DURATION, Constants.RETENTION_POLICY_REPLICATION.toInt, true)
        }
    }

}