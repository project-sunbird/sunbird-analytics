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

case class AssetSnapshotAlgoOutput(partner_id: String, totalImageCount: Long, usedImageCount: Long, totalAudioCount: Long, usedAudioCount: Long, totalQCount: Long, usedQCount: Long, totalActCount: Long, usedActCount: Long, totalTempCount: Long, usedTempCount: Long) extends AlgoOutput

object AssetSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AssetSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "AssetSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.AssetSnapshotSummaryModel";
    val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
        "user" -> AppConf.getConfig("neo4j.bolt.user"),
        "password" -> AppConf.getConfig("neo4j.bolt.password"));

    private def getMediaMap(query: String)(implicit sc: SparkContext): Map[String, Long] = {
        GraphQueryDispatcher.dispatch(graphDBConfig, query).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get("mediaType").asString(), x.get("count").asLong()) }.toMap
    }

    private def getAssetCount(query: String)(implicit sc: SparkContext): Long = {
        GraphQueryDispatcher.dispatch(graphDBConfig, query).list().get(0).get("count").asLong();
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotAlgoOutput] = {

        val mediaTotalMap = getMediaMap(CypherQueries.ASSET_SNAP_MEDIA_TOTAL)
        val totalImageCount = mediaTotalMap.getOrElse("image", 0L)
        val totalAudioCount = mediaTotalMap.getOrElse("audio", 0L)

        val mediaUsedMap = getMediaMap(CypherQueries.ASSET_SNAP_MEDIA_USED)
        val usedImageCount = mediaUsedMap.getOrElse("image", 0L)
        val usedAudioCount = mediaUsedMap.getOrElse("audio", 0L)

        val totalQCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_QUESTION);
        val usedQCount = getAssetCount(CypherQueries.ASSET_SNAP_USED_QUESTION);
        val totalActCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_ACTIVITIES);
        val usedActCount = getAssetCount(CypherQueries.ASSET_SNAP_USED_ACTIVITIES);
        val totalTempCount = getAssetCount(CypherQueries.ASSET_SNAP_TOTAL_TEMPLATES);
        val usedTempCount = getAssetCount(CypherQueries.ASSET_SNAP_USED_TEMPLATES);

        val assetSnapshot = AssetSnapshotAlgoOutput("all", totalImageCount, usedImageCount, totalAudioCount, usedAudioCount, totalQCount, usedQCount, totalActCount, usedActCount, totalTempCount, usedTempCount);
        sc.makeRDD(Seq(assetSnapshot))
    }
    override def postProcess(data: RDD[AssetSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_ASSET_SNAPSHOT_SUMMARY", x.partner_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()));

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
            MeasuredEvent("ME_ASSET_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "AssetSnapshotSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.partner_id)),
                MEEdata(measures));
        }
    }
}