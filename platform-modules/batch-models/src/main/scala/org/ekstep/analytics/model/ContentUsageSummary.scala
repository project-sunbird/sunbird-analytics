package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger

case class ContentUsageMetrics(total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, content_type: String, mime_type: String)

object ContentUsageSummary extends IBatchModel[MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentUsageSummary"
    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.debug("### Execute method started ###", className);
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val contentSessions = filteredEvents.map { event =>
            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val content_id = event.dimensions.gdata.get.id
            val group_user = event.dimensions.group_user.get
            ((content_id, group_user), Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);

        val contentUsage = contentSessions.mapValues { events =>

            val firstEvent = events.sortBy { x => x.context.date_range.from }.head;
            val lastEvent = events.sortBy { x => x.context.date_range.to }.last;
            val date_range = DtRange(firstEvent.syncts, lastEvent.syncts);
            val content_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentType").get.asInstanceOf[String]
            val mime_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("mimeType").get.asInstanceOf[String]

            val total_ts = CommonUtil.roundDouble(events.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2);
            val total_sessions = events.size
            val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
            val total_interactions = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum
            val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
            (ContentUsageMetrics(total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, content_type, mime_type), date_range)
        }

        JobLogger.debug("### Execute method ended ###", className);
        contentUsage.map { f =>
            getMeasuredEvent((f._1._1, f._1._2, f._2._1), configMapping.value, f._2._2);
        }.map { x => JSONUtils.serialize(x) };
    }
    
    private def getMeasuredEvent(contentSumm: (String, Boolean, ContentUsageMetrics), config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {

        val contentUsage = contentSumm._3
        val mid = CommonUtil.getMessageId("ME_CONTENT_USAGE_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange, contentSumm._1 + contentSumm._2);
        val measures = Map(
            "total_ts" -> contentUsage.total_ts,
            "total_sessions" -> contentUsage.total_sessions,
            "avg_ts_session" -> contentUsage.avg_ts_session,
            "total_interactions" -> contentUsage.total_interactions,
            "avg_interactions_min" -> contentUsage.avg_interactions_min,
            "content_type" -> contentUsage.content_type,
            "mime_type" -> contentUsage.mime_type);
        MeasuredEvent("ME_CONTENT_USAGE_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, "", Option(contentSumm._1), None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentUsageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], dtRange),
            Dimensions(None, None, None, None, None, None, None, Option(contentSumm._2)),
            MEEdata(measures));
    }
}