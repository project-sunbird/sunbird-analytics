package org.ekstep.analytics.updater

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.connector.InfluxDB._
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.Period.DAY
import org.ekstep.analytics.framework.Period.MONTH
import org.ekstep.analytics.framework.Period.WEEK
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.CypherQueries
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

import com.datastax.spark.connector._
import com.datastax.spark.connector.toRDDFunctions
import org.ekstep.analytics.framework.conf.AppConf

case class TemplateSnapshotMetrics(d_period: Int, d_template_id: String, d_app_id: String, d_channel: String, template_name: String, category: String, author_id: String, content_count: Long, content_count_start: Long, question_count: Long, question_count_start: Long, updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with Output
case class TemplateMetrics(d_template_id: String, d_app_id: String, d_channel: String, template_name: String, category: String, author_id: String, content_count: Long, question_count: Long)
case class TemplateSnapshotIndex(d_period: Int, d_template_id: String, d_app_id: String, d_channel: String)
object UpdateTemplateSnapshotDB extends IBatchModelTemplate[Empty, Empty, TemplateSnapshotMetrics, TemplateSnapshotMetrics] with IInfluxDBUpdater with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateTemplateSnapshotDB";
    override def name: String = "UpdateTemplateSnapshotDB";

    val TEMPLATE_SNAPSHOT_METRICS = "template_snapshot_metrics";
    val periodsList = List(DAY, WEEK, MONTH);
    val noValue = "None"

    private def getTemplateMetrics(query: String)(implicit sc: SparkContext): List[TemplateMetrics] = {
        val defaultAppId = AppConf.getConfig("default.creation.app.id");
        val defaultChannel = AppConf.getConfig("default.channel.id");
        GraphQueryDispatcher.dispatch(query).list().map { x =>
            TemplateMetrics(x.get("template_id").asString(), x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("name").asString(), x.get("category").asList().mkString(","), x.get("author").asString(), x.get("contentCount").asLong(), x.get("questionCount").asLong())
        }.toList
    }
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        GraphQueryDispatcher.dispatch(CypherQueries.TEMPLATE_ASSES_REL_CREATION)
        GraphQueryDispatcher.dispatch(CypherQueries.TEMPLATE_CONTENT_REL_CREATION)

        data
    }
    override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TemplateSnapshotMetrics] = {

        val metrics = getTemplateMetrics(CypherQueries.TEMPLATE_SNAPSHOT_METIRCS)
        val currentData = sc.parallelize(metrics.map { x =>
            for (p <- periodsList) yield {
                val d_period = CommonUtil.getPeriod(System.currentTimeMillis(), p);
                (TemplateSnapshotIndex(d_period, x.d_template_id, x.d_app_id, x.d_channel), x);
            }
        }.flatMap(f => f))

        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[TemplateSnapshotMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEMPLATE_SNAPSHOT_METRICS_TABLE).on(SomeColumns("d_period", "d_template_id", "d_app_id", "d_channel"));
        val joinedData = currentData.leftOuterJoin(prvData)

        joinedData.map { x =>
            val prevSumm = x._2._2.getOrElse(null)
            val period = x._1.d_period
            val template_id = x._1.d_template_id
            val template_name = x._2._1.template_name
            val author_id = x._2._1.author_id
            val category = x._2._1.category
            val content_count = x._2._1.content_count
            val question_count = x._2._1.question_count
            if (prevSumm == null) {
                TemplateSnapshotMetrics(period, template_id, x._1.d_app_id, x._1.d_channel, template_name, category, author_id, content_count, content_count, question_count, question_count)
            } else {
                TemplateSnapshotMetrics(period, template_id, x._1.d_app_id, x._1.d_channel, template_name, category, author_id, content_count, prevSumm.content_count_start, question_count, prevSumm.question_count_start)
            }
        }
    }

    override def postProcess(data: RDD[TemplateSnapshotMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TemplateSnapshotMetrics] = {
        data.saveToCassandra(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEMPLATE_SNAPSHOT_METRICS_TABLE);
        saveToInfluxDB(data);
        data;
    }

    private def saveToInfluxDB(data: RDD[TemplateSnapshotMetrics])(implicit sc: SparkContext) {
        val metrics = data.map { x =>
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "template_id" -> x.d_template_id, "app_id" -> x.d_app_id, "channel" -> x.d_channel), Map("author_id" -> x.author_id, "category" -> x.category, "template_name" -> x.template_name, "content_count" -> x.content_count.asInstanceOf[AnyRef], "content_count_start" -> x.content_count_start.asInstanceOf[AnyRef], "question_count_start" -> x.question_count_start.asInstanceOf[AnyRef], "question_count" -> x.question_count.asInstanceOf[AnyRef]), time._1);
        }
        val authors = getDenormalizedData("User", data.map { x => x.author_id })
        metrics.denormalize("author_id", "author_name", authors).saveToInflux(TEMPLATE_SNAPSHOT_METRICS);

    }

}