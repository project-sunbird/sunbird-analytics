package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.CommonUtil

case class DeviceContentUsageSummaryInput(did: String, contentId: String, data: Buffer[DerivedEvent]) extends AlgoInput
case class DeviceContentSummary(did: String, content_id: String, gameVer: String, num_sessions: Long, total_interactions: Long, avg_interactions_min: Double, 
                                total_timespent: Double, last_played_on: Long, start_time: Long, 
                                mean_play_time_interval: Double, downloaded: Boolean, download_date: Long, group_user: Boolean) extends AlgoOutput

object DeviceContentUsageSummary extends IBatchModelTemplate[DerivedEvent, DeviceContentUsageSummaryInput, DeviceContentSummary, MeasuredEvent] with Serializable {
  
    val className = "org.ekstep.analytics.model.DeviceContentUsageSummary"
    override def name: String = "DeviceContentUsageSummarizer"
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContentUsageSummaryInput] = {
        
        val filteredEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val devicecontentSessions = filteredEvents.map { event =>
            val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
            val did = event.dimensions.did.get
            val content_id = event.dimensions.gdata.get.id
            ((did, content_id), Buffer(event));
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        devicecontentSessions.map(f => DeviceContentUsageSummaryInput(f._1._1, f._1._2, f._2));
    }
    
    override def algorithm(data: RDD[DeviceContentUsageSummaryInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceContentSummary] = {

        data.map{ dcusEvent =>
            val firstEvent = dcusEvent.data.sortBy { x => x.context.date_range.from }.head;
            val lastEvent = dcusEvent.data.sortBy { x => x.context.date_range.to }.last;
            val gameVer = firstEvent.dimensions.gdata.get.ver
            val num_sessions = dcusEvent.data.size
            val total_timespent = CommonUtil.roundDouble(dcusEvent.data.map { x => (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double]) }.sum, 2);
            val total_interactions = dcusEvent.data.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("noOfInteractEvents").get.asInstanceOf[Int] }.sum
            val avg_interactions_min = if (total_interactions == 0 || total_timespent == 0) 0d else CommonUtil.roundDouble(BigDecimal(total_interactions / (total_timespent / 60)).toDouble, 2);
            val last_played_on = lastEvent.context.date_range.to
            val start_time = firstEvent.context.date_range.from
            val timeDiff = CommonUtil.getTimeDiff(start_time, last_played_on).get
            val play_time_interval = timeDiff - total_timespent
            val mean_play_time_interval	= if(num_sessions < 2) 0d else CommonUtil.roundDouble(BigDecimal(play_time_interval / (num_sessions - 1)).toDouble, 2)
            val group_user = firstEvent.dimensions.group_user.get
            DeviceContentSummary(dcusEvent.did, dcusEvent.contentId, gameVer, num_sessions, total_interactions, avg_interactions_min, total_timespent, last_played_on, start_time, mean_play_time_interval,false , 0l, group_user)
        }
    }   
    
    override def postProcess(data: RDD[DeviceContentSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        
        data.map { dcuSummary =>
            val mid = CommonUtil.getMessageId("ME_DEVICE_CONTENT_USAGE_SUMMARY", dcuSummary.did, config.getOrElse("granularity", "DAY").asInstanceOf[String], DtRange(dcuSummary.start_time, dcuSummary.last_played_on), dcuSummary.content_id );
            val measures = Map(
                "num_sessions" -> dcuSummary.num_sessions,
                "total_timespent" -> dcuSummary.total_timespent,
                "avg_interactions_min" -> dcuSummary.avg_interactions_min,
                "total_interactions" -> dcuSummary.total_interactions,
                "last_played_on" -> dcuSummary.last_played_on,
                "start_time" -> dcuSummary.start_time,
                "mean_play_time_interval" -> dcuSummary.mean_play_time_interval,
                "downloaded" -> dcuSummary.downloaded,
                "download_date" -> dcuSummary.download_date,
                "group_user" -> dcuSummary.group_user);
            MeasuredEvent("ME_DEVICE_CONTENT_USAGE_SUMMARY", System.currentTimeMillis(), dcuSummary.last_played_on, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "DeviceContentUsageSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], DtRange(dcuSummary.start_time, dcuSummary.last_played_on)),
                Dimensions(None, Option(dcuSummary.did), Option(new GData(dcuSummary.content_id, dcuSummary.gameVer)), None, None, None, None, None),
                MEEdata(measures));
        }
    }
}