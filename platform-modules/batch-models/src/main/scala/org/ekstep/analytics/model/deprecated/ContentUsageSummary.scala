package org.ekstep.analytics.model.deprecated

import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.BigDecimal

case class ContentUsageMetrics(content_id: String, content_ver: String, is_group_user: Boolean, total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, content_type: String, mime_type: String, dt_range: DtRange) extends AlgoOutput;
case class ContentUsageSummaryInput(contentId: String, isGroupUser: Boolean, data: Buffer[DerivedEvent]) extends AlgoInput;

@deprecated
object ContentUsageSummary extends IBatchModelTemplate[DerivedEvent, ContentUsageSummaryInput, ContentUsageMetrics, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentUsageSummary"
    override def name: String = "ContentUsageSummarizer"
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentUsageSummaryInput] = {
        val configMapping = sc.broadcast(config);

        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val contentSessions = filteredEvents.map { event =>
            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val content_id = event.dimensions.gdata.get.id
            val group_user = event.dimensions.group_user.get
            ((content_id, group_user), Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        contentSessions.map(f => ContentUsageSummaryInput(f._1._1, f._1._2, f._2));
    }

    override def algorithm(data: RDD[ContentUsageSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentUsageMetrics] = {

        data.map { cusEvent =>
            val firstEvent = cusEvent.data.sortBy { x => x.context.date_range.from }.head;
            val lastEvent = cusEvent.data.sortBy { x => x.context.date_range.to }.last;
            val date_range = DtRange(firstEvent.syncts, lastEvent.syncts);
            val content_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentType").get.asInstanceOf[String]
            val mime_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("mimeType").get.asInstanceOf[String]
            val content_ver = firstEvent.dimensions.gdata.get.ver
            
            val total_ts = CommonUtil.roundDouble(cusEvent.data.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2);
            val total_sessions = cusEvent.data.size
            val avg_ts_session = CommonUtil.roundDouble((total_ts / total_sessions), 2)
            val total_interactions = cusEvent.data.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum
            val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
            ContentUsageMetrics(cusEvent.contentId, content_ver, cusEvent.isGroupUser, total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, content_type, mime_type, date_range)
        }
    }

    override def postProcess(data: RDD[ContentUsageMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { cuMetrics =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_USAGE_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range, cuMetrics.content_id + cuMetrics.is_group_user);
            val measures = Map(
                "total_ts" -> cuMetrics.total_ts,
                "total_sessions" -> cuMetrics.total_sessions,
                "avg_ts_session" -> cuMetrics.avg_ts_session,
                "total_interactions" -> cuMetrics.total_interactions,
                "avg_interactions_min" -> cuMetrics.avg_interactions_min,
                "content_type" -> cuMetrics.content_type,
                "mime_type" -> cuMetrics.mime_type);
            MeasuredEvent("ME_CONTENT_USAGE_SUMMARY", System.currentTimeMillis(), cuMetrics.dt_range.to, "1.0", mid, "", Option(cuMetrics.content_id), None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentUsageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], cuMetrics.dt_range),
                Dimensions(None, None, Option(new GData(cuMetrics.content_id, cuMetrics.content_ver)), None, None, None, None, Option(cuMetrics.is_group_user)),
                MEEdata(measures));
        }

    }

}