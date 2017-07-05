package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.DerivedEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher
import org.ekstep.analytics.connector.InfluxDB._
import org.ekstep.analytics.framework.CassandraTable

case class AssetSnapshotSummary(d_period: Int, d_partner_id: String, d_app_id: String, d_channel: String, total_images_count: Long, total_images_count_start: Long, used_images_count: Long, used_images_count_start: Long, total_audio_count: Long, total_audio_count_start: Long, used_audio_count: Long, used_audio_count_start: Long, total_questions_count: Long, total_questions_count_start: Long, used_questions_count: Long, used_questions_count_start: Long, total_activities_count: Long, total_activities_count_start: Long, used_activities_count: Long, used_activities_count_start: Long, total_templates_count: Long, total_templates_count_start: Long, used_templates_count: Long, used_templates_count_start: Long, updated_date: Option[DateTime] = Option(DateTime.now())) extends AlgoOutput with Output with CassandraTable
case class AssetSnapshotKey(d_period: Int, d_partner_id: String, d_app_id: String, d_channel: String)

object UpdateAssetSnapshotDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AssetSnapshotSummary, AssetSnapshotSummary] with IInfluxDBUpdater with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateAssetSnapshotDB"
    override def name: String = "UpdateAssetSnapshotDB"
    val ASSET_SNAPSHOT_METRICS = "asset_snapshot_metrics";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }
    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotSummary] = {

        val periodsList = List(DAY, WEEK, MONTH)
        val currentData = data.map { x =>
            for (p <- periodsList) yield {
                val d_period = CommonUtil.getPeriod(x.syncts, p);
                val appId = CommonUtil.getAppDetails(x).id
                val channel = CommonUtil.getChannelId(x)
                (AssetSnapshotKey(d_period, x.dimensions.partner_id.get, appId, channel), x);
            }
        }.flatMap(f => f)

        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[AssetSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY).on(SomeColumns("d_period", "d_partner_id", "d_app_id", "d_channel"));
        val joinedData = currentData.leftOuterJoin(prvData)

        joinedData.map { f =>
            val prevSumm = f._2._2.getOrElse(null)
            val eksMap = f._2._1.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val total_images_count = eksMap.get("total_images_count").get.asInstanceOf[Number].longValue
            val used_images_count = eksMap.get("used_images_count").get.asInstanceOf[Number].longValue
            val total_audio_count = eksMap.get("total_audio_count").get.asInstanceOf[Number].longValue
            val used_audio_count = eksMap.get("used_audio_count").get.asInstanceOf[Number].longValue
            val total_questions_count = eksMap.get("total_questions_count").get.asInstanceOf[Number].longValue
            val used_questions_count = eksMap.get("used_questions_count").get.asInstanceOf[Number].longValue
            val total_activities_count = eksMap.get("total_activities_count").get.asInstanceOf[Number].longValue
            val used_activities_count = eksMap.get("used_activities_count").get.asInstanceOf[Number].longValue
            val total_templates_count = eksMap.get("total_templates_count").get.asInstanceOf[Number].longValue
            val used_templates_count = eksMap.get("used_templates_count").get.asInstanceOf[Number].longValue

            if (null == prevSumm)
                AssetSnapshotSummary(f._1.d_period, f._1.d_partner_id, f._1.d_app_id, f._1.d_channel, total_images_count, total_images_count, used_images_count, used_images_count, total_audio_count, total_audio_count, used_audio_count, used_audio_count, total_questions_count, total_questions_count, used_questions_count, used_questions_count, total_activities_count, total_activities_count, used_activities_count, used_activities_count, total_templates_count, total_templates_count, used_templates_count, used_templates_count)
            else
                AssetSnapshotSummary(f._1.d_period, f._1.d_partner_id, f._1.d_app_id, f._1.d_channel, total_images_count, prevSumm.total_images_count_start, used_images_count, prevSumm.used_images_count_start, total_audio_count, prevSumm.total_audio_count_start, used_audio_count, prevSumm.used_audio_count_start, total_questions_count, prevSumm.total_questions_count_start, used_questions_count, prevSumm.used_questions_count_start, total_activities_count, prevSumm.total_activities_count_start, used_activities_count, prevSumm.used_activities_count_start, total_templates_count, prevSumm.total_templates_count_start, used_templates_count, prevSumm.used_templates_count_start)
        }
    }
    override def postProcess(data: RDD[AssetSnapshotSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotSummary] = {
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY);
        saveToInfluxDB(data);
        data;
    }

    private def saveToInfluxDB(data: RDD[AssetSnapshotSummary])(implicit sc: SparkContext) {
        val metrics = data.map { x =>
            val fields = (CommonUtil.caseClassToMap(x) - ("d_period", "d_partner_id", "d_app_id", "d_channel", "updated_date")).map(f => (f._1, f._2.asInstanceOf[Number].doubleValue().asInstanceOf[AnyRef]));
            val time = getDateTime(x.d_period);
            InfluxRecord(Map("period" -> time._2, "partner_id" -> x.d_partner_id, "app_id" -> x.d_app_id, "channel" -> x.d_channel), fields, time._1);
        };
        val partners = getDenormalizedData("Partner", data.map { x => x.d_partner_id })
        metrics.denormalize("partner_id", "partner_name", partners).saveToInflux(ASSET_SNAPSHOT_METRICS);
    }

}