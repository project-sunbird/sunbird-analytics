package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.CypherQueries

case class ConceptSnapshotAlgoOutput(concept_id: String, total_content_count: Long, live_content_count: Long, review_content_count: Long) extends AlgoOutput

object ConceptSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ConceptSnapshotAlgoOutput, MeasuredEvent] with Serializable {
  
    override def name(): String = "ConceptSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.ConceptSnapshotSummaryModel";
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSnapshotAlgoOutput] = {

        val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val concept_total_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONCEPT_SNAPSHOT_TOTAL_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnc.IL_UNIQUE_ID").asString(), x.get("cnc.contentCount").asLong())}
        val concept_live_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONCEPT_SNAPSHOT_LIVE_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnc.IL_UNIQUE_ID").asString(), x.get("cnc.liveContentCount").asLong())}
        val concept_review_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONCEPT_SNAPSHOT_REVIEW_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnc.IL_UNIQUE_ID").asString(), x.get("rcc").asLong())}
        
        val totalContentRDD = sc.parallelize(concept_total_content_count)
        val liveContentRDD = sc.parallelize(concept_live_content_count)
        val reviewContentRDD = sc.parallelize(concept_review_content_count)
        val rdd = totalContentRDD.leftOuterJoin(liveContentRDD).leftOuterJoin(reviewContentRDD).map{f =>
            ConceptSnapshotAlgoOutput(f._1, f._2._1._1, f._2._1._2.getOrElse(0), f._2._2.getOrElse(0))
        }
        rdd;
    }

    override def postProcess(data: RDD[ConceptSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_CONCEPT_SNAPSHOT_SUMMARY", x.concept_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()));
            val measures = Map(
                "total_content_count" -> x.total_content_count,
                "live_content_count" -> x.live_content_count,
                "review_content_count" -> x.review_content_count);
            MeasuredEvent("ME_CONCEPT_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ConceptSnapshotSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.concept_id)),
                MEEdata(measures));
        }
    }

}