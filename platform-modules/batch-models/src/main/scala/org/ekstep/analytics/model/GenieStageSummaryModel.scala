package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.util.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.SessionBatchModel

case class GenieStageAlgoOut(stageId: String, sid: String, timeSpent: Double, visitCount: Int, interactEventsCount: Int, interactEvents: List[Map[String, String]], dt_range: DtRange, did: String, tags: Option[AnyRef], syncts: Long) extends AlgoOutput

object GenieStageSummaryModel extends SessionBatchModel[DerivedEvent, MeasuredEvent] with IBatchModelTemplate[DerivedEvent, DerivedEvent, GenieStageAlgoOut, MeasuredEvent] with Serializable {
  
    implicit val className = "org.ekstep.analytics.model.GenieStageSummaryModel"
    override def name(): String = "GenieStageSummaryModel";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_LAUNCH_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieStageAlgoOut] = {
        data.map { event =>
            val screenSummaries = event.edata.eks.screenSummary;
            
            val did = event.dimensions.did
            val tags = event.tags
            
            if (null != screenSummaries && screenSummaries.size > 0) {
                screenSummaries.map { x =>
                    val ss = JSONUtils.deserialize[GenieStageSummary](JSONUtils.serialize(x));
                    GenieStageAlgoOut(ss.stageId, ss.sid, ss.timeSpent, ss.visitCount, ss.interactEventsCount, ss.interactEvents, event.context.date_range, did, Option(tags), event.syncts)
                }
            } else {
                Array[GenieStageAlgoOut]();
            }
        }.filter { x => !x.isEmpty }.flatMap { x => x.map { x => x } }
    }

    override def postProcess(data: RDD[GenieStageAlgoOut], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_STAGE_SUMMARY", summary.stageId + summary.sid, config.getOrElse("granularity", "GENIE SESSION").asInstanceOf[String], summary.dt_range.to);
            val measures = Map(
                "timeSpent" -> summary.timeSpent,
                "stageVisitCount" -> summary.visitCount,
                "interactEventsCount" -> summary.interactEventsCount,
                "interactEvents" -> summary.interactEvents);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieStageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_GENIE_STAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, null, None, None,
                Context(pdata, None, "GENIE SESSION", summary.dt_range),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None, None, None, None, None, None, Option(summary.sid), Option(summary.stageId)), MEEdata(measures), summary.tags);
        };
    }
}