package org.ekstep.analytics.model

import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.Empty
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

case class AssetSnapshotAlgoOutput(partner_id: String, totalImageCount: Long, usedImageCount: Long, totalAudioCount: Long, usedAudioCount: Long, totalQCount: Long, usedQCount: Long, totalActCount: Long, usedActCount: Long, totalTempCount: Long, usedTempCount: Long) extends AlgoOutput

object AssetSnapshotSummaryModel extends IBatchModelTemplate[Empty, Empty, AssetSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "AssetSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.AssetSnapshotSummaryModel";

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        sc.makeRDD(List(Empty()));
    }

    override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotAlgoOutput] = {

        val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val totalImageCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_TOTAL_IMAGE).list().get(0).get("count(img)").asLong();
        val usedImageCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_USED_IMAGE).list().get(0).get("count(distinct img)").asLong();
        val totalAudioCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_TOTAL_AUDIO).list().get(0).get("count(aud)").asLong();
        val usedAudioCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_USED_AUDIO).list().get(0).get("count(distinct aud)").asLong();
        val totalQCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_TOTAL_QUESTION).list().get(0).get("count(as)").asLong();
        val usedQCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_USED_QUESTION).list().get(0).get("count(distinct as)").asLong();
        val totalActCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_TOTAL_ACTIVITIES).list().get(0).get("count(act)").asLong();
        val usedActCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_USED_ACTIVITIES).list().get(0).get("count(distinct act)").asLong();
        val totalTempCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_TOTAL_TEMPLATES).list().get(0).get("count(temp)").asLong();
        val usedTempCount = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.ASSET_SNAP_USED_TEMPLATES).list().get(0).get("count(distinct temp)").asLong();

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