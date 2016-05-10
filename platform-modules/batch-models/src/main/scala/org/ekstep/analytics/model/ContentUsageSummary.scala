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

case class ContentUsage(total_ts: Double, total_sessions: Int, avg_ts_session: Double, total_interactions: Long, avg_interactions_min: Double, content_type: String, mime_type: String, game_version: String)

object ContentUsageSummary extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val events = data.map { event =>
            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val content_id = event.dimensions.gdata.get.id
            val partner_id = eksMap.get("partnerId").get.asInstanceOf[String]
            val group_user = eksMap.get("groupUser").get.asInstanceOf[String]
            ((content_id, partner_id, group_user), Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);

//        val contentUsage = events.mapValues { events =>
//
//            val firstEvent = events.sortBy { x => x.context.date_range.from }.head;
//            val lastEvent = events.sortBy { x => x.context.date_range.to }.last;
//            val date_range = DtRange(firstEvent.syncts, lastEvent.syncts);
//
//            val gameVersion = firstEvent.dimensions.gdata.get.ver
//            val content_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentType").get.asInstanceOf[String]
//            val mime_type = firstEvent.edata.eks.asInstanceOf[Map[String, AnyRef]].get("mimeType").get.asInstanceOf[String]
//
//            val total_ts = events.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum;
//            val total_sessions = events.size
//            val avg_ts_session = (total_ts / total_sessions)
//            val total_interactions = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum
//            val avg_interactions_min = if (total_interactions == 0 || total_ts == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_ts / 60)).toDouble, 2);
//            (ContentUsage(total_ts, total_sessions, avg_ts_session, total_interactions, avg_interactions_min, content_type, mime_type, gameVersion), date_range)
//        }

        events.map { f =>
            JSONUtils.serialize(f)
        };
    }
}