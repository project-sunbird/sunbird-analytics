package org.ekstep.analytics.model

import org.ekstep.analytics.framework._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.CommonUtil

case class DeviceIndex(device_id: String, channel: String)
case class DialStats(total_count: Long, success_count: Long, failure_count: Long)
case class DeviceInput(index: DeviceIndex, wfsData: Option[Buffer[DerivedEvent]], rawData: Option[Buffer[V3Event]]) extends AlgoInput
case class DeviceSummary(device_id: String, channel: String, total_ts: Double, total_launches: Long, contents_played: Long, unique_contents_played: Long, content_downloads: Long, dial_stats: DialStats, dt_range: DtRange, syncts: Long) extends AlgoOutput
case class EventData(event: String) extends Input

object DeviceSummaryModel extends IBatchModelTemplate[EventData, DeviceInput, DeviceSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.DeviceSummaryModel"
    override def name: String = "DeviceSummaryModel"

    override def preProcess(data: RDD[EventData], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceInput] = {
        val rawEventsList = List("SEARCH", "INTERACT")
        val wfsData = data.filter(f => f.event.contains("ME_WORKFLOW_SUMMARY")).map(f => JSONUtils.deserialize[DerivedEvent](f.event)).filter { x => x.dimensions.did.nonEmpty }
        val rawData = data.filter(f => !f.event.contains("ME_WORKFLOW_SUMMARY")).map(f => JSONUtils.deserialize[V3Event](f.event)).filter{f => rawEventsList.contains(f.eid)}.filter { x => ((x.context.did.nonEmpty) && (StringUtils.equals(x.edata.subtype, "ContentDownload-Success") || x.edata.filters.getOrElse(Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]].contains("dialcodes"))) };
        val groupedWfsData = wfsData.map { event =>
            (DeviceIndex(event.dimensions.did.get, event.dimensions.channel.get), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        val groupedRawData = rawData.map { event =>
            (DeviceIndex(event.context.did.get, event.context.channel), Buffer(event))
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b);
        groupedWfsData.fullOuterJoin(groupedRawData).map(f => DeviceInput(f._1, f._2._1, f._2._2))
    }

    override def algorithm(data: RDD[DeviceInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceSummary] = {        
        data.map{ f => 
            val index = f.index
            val wfs = f.wfsData.getOrElse(Buffer()).sortBy { x => x.context.date_range.from }
            val raw = f.rawData.getOrElse(Buffer())
            val startTimestamp = wfs.head.context.date_range.from;
            val endTimestamp = wfs.last.context.date_range.to;
            val syncts = wfs.last.syncts
            val total_ts = if (wfs.size < 1) 0.0 else wfs.map { x =>
                (x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("time_spent").get.asInstanceOf[Double])
            }.sum
            val total_launches = if (wfs.size < 1) 0L else wfs.filter(f => "app".equals(f.dimensions.`type`.getOrElse(""))).length
            val content_play_events = wfs.filter(f => ("content".equals(f.dimensions.`type`.getOrElse("")) && ("play".equals(f.dimensions.mode.getOrElse("")))))
            val contents_played = content_play_events.length
            val unique_contents_played = content_play_events.map(f => f.`object`.getOrElse(V3Object("", "", None, None)).id).distinct.filter(f => f.nonEmpty).length
            val dialcodes_events = raw.filter(f => "SEARCH".equals(f.eid))
            val dial_count = dialcodes_events.length
            val dial_success = dialcodes_events.filter(f => f.edata.size > 0).length
            val dial_failure = dialcodes_events.filter(f => f.edata.size == 0).length
            val content_downloads = raw.filter(f => "INTERACT".equals(f.eid)).length
            DeviceSummary(index.device_id, index.channel, CommonUtil.roundDouble(total_ts, 2), total_launches, contents_played, unique_contents_played, content_downloads, DialStats(dial_count, dial_success, dial_failure), DtRange(startTimestamp, endTimestamp), syncts)
        }        
    }

    override def postProcess(data: RDD[DeviceSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {        
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_DEVICE_SUMMARY", x.device_id, "DAY", x.dt_range, "NA", None, Option(x.channel));
            val measures = Map(
                "total_ts" -> x.total_ts,
                "total_launches" -> x.total_launches,
                "contents_played" -> x.contents_played,
                "unique_contents_played" -> x.unique_contents_played,
                "content_downloads" -> x.content_downloads,
                "dial_stats" -> x.dial_stats);
            MeasuredEvent("ME_DEVICE_SUMMARY", System.currentTimeMillis(), x.syncts, "1.0", mid, null, null, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "DeviceSummary").asInstanceOf[String])), None, "DAY", x.dt_range),
                Dimensions(None, Option(x.device_id), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.channel)),
                MEEdata(measures));
        }       
    }
}