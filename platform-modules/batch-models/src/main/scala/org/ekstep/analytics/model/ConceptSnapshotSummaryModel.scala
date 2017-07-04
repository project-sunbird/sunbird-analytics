package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.CypherQueries

case class ConceptSnapshotAlgoOutput(concept_id: String, app_id: String, channel: String, total_content_count: Long, live_content_count: Long, review_content_count: Long) extends AlgoOutput
case class ConceptSnapshotIndex(concept_id: String, app_id: String, channel: String)

object ConceptSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ConceptSnapshotAlgoOutput, MeasuredEvent] with Serializable {
  
    override def name(): String = "ConceptSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.ConceptSnapshotSummaryModel";
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSnapshotAlgoOutput] = {

        val totalContentRDD = sc.parallelize(getConceptsCount(CypherQueries.CONCEPT_SNAPSHOT_TOTAL_CONTENT_COUNT))
        val liveContentRDD = sc.parallelize(getConceptsCount(CypherQueries.CONCEPT_SNAPSHOT_LIVE_CONTENT_COUNT))
        val reviewContentRDD = sc.parallelize(getConceptsCount(CypherQueries.CONCEPT_SNAPSHOT_REVIEW_CONTENT_COUNT))
        val rdd = totalContentRDD.fullOuterJoin(liveContentRDD).fullOuterJoin(reviewContentRDD).map{f =>
            val totalCount = f._2._1.getOrElse(Option(0L), Option(0L))._1.get
            val liveCount = f._2._1.getOrElse(Option(0L), Option(0L))._2.get
            val reviewCount = f._2._2.getOrElse(0L)
            ConceptSnapshotAlgoOutput(f._1.concept_id, f._1.app_id, f._1.channel, totalCount, liveCount, reviewCount)
        }
        rdd;
    }

    override def postProcess(data: RDD[ConceptSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_CONCEPT_SNAPSHOT_SUMMARY", x.concept_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()), "NA", Option(x.app_id), Option(x.channel));
            val measures = Map(
                "total_content_count" -> x.total_content_count,
                "live_content_count" -> x.live_content_count,
                "review_content_count" -> x.review_content_count);
            MeasuredEvent("ME_CONCEPT_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ConceptSnapshotSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.concept_id), Option(x.app_id), None, None, Option(x.channel)),
                MEEdata(measures));
        }
    }

    private def getConceptsCount(query: String)(implicit sc: SparkContext): Array[(ConceptSnapshotIndex, Long)] = {
    	GraphQueryDispatcher.dispatch(query).list().toArray()
    	.map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (ConceptSnapshotIndex(x.get("identifier").asString(), x.get("appId").asString(), x.get("channel").asString()), x.get("count").asLong())}
    }
    
}