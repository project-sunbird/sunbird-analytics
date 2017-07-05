package org.ekstep.analytics.updater

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.connector.InfluxDB._
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.Period.DAY
import org.ekstep.analytics.framework.Period.MONTH
import org.ekstep.analytics.framework.Period.WEEK
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.CypherQueries
import org.joda.time.DateTime

import com.datastax.spark.connector._
import com.datastax.spark.connector.toRDDFunctions

case class PluginMetrics(d_plugin_id: String, plugin_name: String, category: String, author: String, content_count: Int)
case class PluginSnapshotMetrics(d_period: Int, d_plugin_id: String, plugin_name: String, category: String, author: String, content_count: Int, content_count_start: Int, updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with Output

case class PluginSnapshotIndex(d_period: Int, d_plugin_id: String)

object UpdatePluginSnapshotDB extends IBatchModelTemplate[Empty, Empty, PluginSnapshotMetrics, PluginSnapshotMetrics] with IInfluxDBUpdater with Serializable {

    val className = "org.ekstep.analytics.updater.UpdatePluginSnapshotDB";
    override def name: String = "UpdatePluginSnapshotDB";
    val periodList = List(DAY, WEEK, MONTH);
    val PLUGIN_SNAPSHOT_METRICS = "plugin_snapshot_metrics";
    val noValue = "None"
    private def getPluginMetrics(query: String)(implicit sc: SparkContext): List[PluginMetrics] = {
        GraphQueryDispatcher.dispatch(query).list().map { x =>
            PluginMetrics(x.get("plugin_id").asString(), x.get("name").asString(), x.get("category").asList().mkString(","), x.get("author").asString(), x.get("contentCount").asInt)
        }.toList
    }
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        data
    }
    override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PluginSnapshotMetrics] = {

        val metrics = getPluginMetrics(CypherQueries.PLUGIN_SNAPSHOT_METIRCS)

        val periodMetrics = metrics.map { x =>
                periodList.map { period =>
                    val ts = CommonUtil.getPeriod(System.currentTimeMillis(), period);
                    (PluginSnapshotIndex(ts, x.d_plugin_id), x);
                }
            }.flatMap(f=>f)

        val currentData = sc.parallelize(periodMetrics)
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[PluginSnapshotMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.PLUGIN_SNAPSHOT_METRICS_TABLE).on(SomeColumns("d_period", "d_plugin_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        joinedData.map { x =>
            val prevSumm = x._2._2.getOrElse(null)
            val period = x._1.d_period
            val pulgin_id = x._1.d_plugin_id
            val plugin_name = x._2._1.plugin_name
            val author = x._2._1.author
            val category = x._2._1.category
            val content_count = x._2._1.content_count
            if (prevSumm == null) {
                PluginSnapshotMetrics(period, pulgin_id, plugin_name, category, author, content_count, content_count)
            } else {
                PluginSnapshotMetrics(period, pulgin_id, plugin_name, category, author, content_count, prevSumm.content_count_start)
            }
        }
    }
    override def postProcess(data: RDD[PluginSnapshotMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PluginSnapshotMetrics] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.PLUGIN_SNAPSHOT_METRICS_TABLE);
        saveToInfluxDB(data);
        data;
    }
    private def saveToInfluxDB(data: RDD[PluginSnapshotMetrics])(implicit sc: SparkContext) {
        val matric = data.map { x =>
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "plugin_id" -> x.d_plugin_id), Map("author" -> x.author, "category" -> x.category, "plugin_name" -> x.plugin_name, "content_count" -> x.content_count.asInstanceOf[AnyRef], "content_count_start" -> x.content_count_start.asInstanceOf[AnyRef]), time._1);
        }
        val authors = getDenormalizedData("User", data.map { x => x.author })
        matric.denormalize("author_id", "author_name", authors).saveToInflux(PLUGIN_SNAPSHOT_METRICS);
    }
}