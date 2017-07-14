package org.ekstep.analytics.model

import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.util.CypherQueries
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.util.JSONUtils

case class AssetSnapshotAlgoOutput(partner_id: String, app_id: String, channel: String, totalImageCount: Long, usedImageCount: Long, totalAudioCount: Long, usedAudioCount: Long, totalQCount: Long, usedQCount: Long, totalActCount: Long, usedActCount: Long, totalTempCount: Long, usedTempCount: Long) extends AlgoOutput
case class AssetSnapshotIndex(app_id: String, channel: String)
case class MediaKey(media_type: String, count: Long)

object AssetSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AssetSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "AssetSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.AssetSnapshotSummaryModel";

    val defaultAppId = AppConf.getConfig("default.creation.app.id");
    val defaultChannel = AppConf.getConfig("default.channel.id");
    
    private def getMediaMap(query: String)(implicit sc: SparkContext): RDD[(AssetSnapshotIndex, Iterable[MediaKey])] = {
        val x = GraphQueryDispatcher.dispatch(query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("mediaType").asString(), x.get("count").asLong()) } //.toMap
        sc.parallelize(x.map(f => (AssetSnapshotIndex(f._1, f._2), MediaKey(f._3, f._4)))).groupByKey()
    }

    private def getAssetCount(query: String)(implicit sc: SparkContext): RDD[(AssetSnapshotIndex, Long)] = {
        val x = GraphQueryDispatcher.dispatch(query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("count").asLong()) };
        sc.parallelize(x.map(f => (AssetSnapshotIndex(f._1, f._2), f._3)))
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotAlgoOutput] = {

        val totalQCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_QUESTION).fullOuterJoin(getAssetCount(CypherQueries.ASSET_SNAP_USED_QUESTION)).map(x => (x._1, (x._2._1.getOrElse(0L), x._2._2.getOrElse(0L))));
        val totalActCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_ACTIVITIES).fullOuterJoin(getAssetCount(CypherQueries.ASSET_SNAP_USED_ACTIVITIES)).map(x => (x._1, (x._2._1.getOrElse(0L), x._2._2.getOrElse(0L))));
        val totalTempCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_TEMPLATES).fullOuterJoin(getAssetCount(CypherQueries.ASSET_SNAP_USED_TEMPLATES)).map(x => (x._1, (x._2._1.getOrElse(0L), x._2._2.getOrElse(0L))));
        
        val rdd1 = totalQCount.fullOuterJoin(totalActCount).fullOuterJoin(totalTempCount).map(x => (x._1, (x._2._1.getOrElse(Option(0L, 0L), Option(0L, 0L))._1.getOrElse(0L,0L), x._2._1.getOrElse(Option(0L, 0L), Option(0L, 0L))._2.getOrElse(0L,0L), x._2._2.getOrElse(0L, 0L))))

        val res = getMediaMap(CypherQueries.ASSET_SNAP_MEDIA_TOTAL).fullOuterJoin(getMediaMap(CypherQueries.ASSET_SNAP_MEDIA_USED)).fullOuterJoin(rdd1).map { f =>
            val totalImageCount = if (f._2._1.isEmpty) 0L else if (f._2._1.get._1.isEmpty) 0L else if (!f._2._1.get._1.get.map(x => x.media_type).toArray.contains("image")) 0L else f._2._1.get._1.get.filter { x => "image".equals(x.media_type) }.head.count
            val totalAudioCount = if (f._2._1.isEmpty) 0L else if (f._2._1.get._1.isEmpty) 0L else if (!f._2._1.get._1.get.map(x => x.media_type).toArray.contains("audio")) 0L else f._2._1.get._1.get.filter { x => "audio".equals(x.media_type) }.head.count
            val usedImageCount = if (f._2._1.isEmpty) 0L else if (f._2._1.get._2.isEmpty) 0L else if (!f._2._1.get._2.get.map(x => x.media_type).toArray.contains("image")) 0L else f._2._1.get._2.get.filter { x => "image".equals(x.media_type) }.head.count
            val usedAudioCount = if (f._2._1.isEmpty) 0L else if (f._2._1.get._2.isEmpty) 0L else if (!f._2._1.get._2.get.map(x => x.media_type).toArray.contains("audio")) 0L else f._2._1.get._2.get.filter { x => "audio".equals(x.media_type) }.head.count
            val QActTempCounts = if(f._2._2.isEmpty) ((0L, 0L),(0L, 0L),(0L, 0L)) else f._2._2.get
            AssetSnapshotAlgoOutput("all", f._1.app_id, f._1.channel, totalImageCount, usedImageCount, totalAudioCount, usedAudioCount, QActTempCounts._1._1, QActTempCounts._1._2, QActTempCounts._2._1, QActTempCounts._2._2, QActTempCounts._3._1, QActTempCounts._3._2);
        }
        val allRDD = sc.parallelize(Seq(AssetSnapshotAlgoOutput("all", "all", "all", res.map(x => x.totalImageCount).sum.toLong, res.map(x => x.usedImageCount).sum.toLong, res.map(x => x.totalAudioCount).sum.toLong, res.map(x => x.usedAudioCount).sum.toLong, res.map(x => x.totalQCount).sum.toLong, res.map(x => x.usedQCount).sum.toLong, res.map(x => x.totalActCount).sum.toLong, res.map(x => x.usedActCount).sum.toLong, res.map(x => x.totalTempCount).sum.toLong, res.map(x => x.usedTempCount).sum.toLong)))
        res ++ (allRDD)
    }
    override def postProcess(data: RDD[AssetSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_ASSET_SNAPSHOT_SUMMARY", x.partner_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()), "NA", Option(x.app_id), Option(x.channel));

            val measures = Map(
                "total_images_count" -> x.totalImageCount,
                "used_images_count" -> x.usedImageCount,
                "total_audio_count" -> x.totalAudioCount,
                "used_audio_count" -> x.usedAudioCount,
                "total_questions_count" -> x.totalQCount,
                "used_questions_count" -> x.usedQCount,
                "total_activities_count" -> x.totalActCount,
                "used_activities_count" -> x.usedActCount,
                "total_templates_count" -> x.totalTempCount,
                "used_templates_count" -> x.usedTempCount);
            MeasuredEvent("ME_ASSET_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), meEventVersion, mid, "", x.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "AssetSnapshotSummarizer").asInstanceOf[String])), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, Option(PData(x.app_id, "1.0")), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.partner_id)),
                MEEdata(measures));
        }
    }
}