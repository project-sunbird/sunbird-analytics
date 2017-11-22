package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JobLogger
import scala.collection.mutable.Buffer
import org.joda.time.DateTime
import org.ekstep.analytics.framework.conf.AppConf

case class ContentSideloading(content_id: String, appId: String, channel: String, num_downloads: Long, total_count: Long, num_sideloads: Long, origin_map: Map[String, Double], avg_depth: Double, updated_date: Option[DateTime] = Option(DateTime.now()))
case class ReducedContentDetails(content_id: String, pdata: PData, channel: String, transfer_count: Double, did: String, origin: String, ts: Long, syncts: Long)
case class ContentSideloadingInput(index: ContentSideloadingIndex, currentDetails: Iterable[ReducedContentDetails], previousDetails: Option[ContentSideloading]) extends AlgoInput
case class ContentSideloadingOutput(summary: ContentSideloading, dtRange: DtRange, synts: Long, pdata: PData) extends AlgoOutput
case class ContentSideloadingIndex(contentId: String, appId: String, channel: String)

object ContentSideloadingSummaryModel extends IBatchModelTemplate[V3Event, ContentSideloadingInput, ContentSideloadingOutput, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ContentSideloadingSummaryModel"
    override def name: String = "ContentSideloadingSummaryModel"
    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSideloadingInput] = {
        val configMapping = sc.broadcast(config);
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("SHARE")));
        val filteredEvents = DataFilter.filter(events, Filter("edata.dir", "IN", Option(List("in", "In")))).filter { x => (x.edata.items.map(f => f.obj.`type`).contains("Content") | x.edata.items.map(f => f.obj.`type`).contains("CONTENT")) };

        val reducedData = filteredEvents.map { event =>
            val items = event.edata.items
            val pdata = CommonUtil.getAppDetails(event)
            val channelId = CommonUtil.getChannelId(event)
            items.map { f =>
                val transferCount = f.params.filter(x => x.isDefinedAt("transfers")).head.get("transfers").get.asInstanceOf[Double]
                ReducedContentDetails(f.obj.id, pdata, channelId, transferCount, event.context.did.getOrElse(""), f.origin.id, event.ets, CommonUtil.getEventSyncTS(event))
            }
        }.flatMap(f => f)
        val contentMap = reducedData.groupBy { x => ContentSideloadingIndex(x.content_id, x.pdata.id, x.channel) }.partitionBy(new HashPartitioner(JobContext.parallelization))
        val prevSideloadingSummary = contentMap.map(f => f._1).joinWithCassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)//.map(f => (f._1.content_id, f._2))
        val joinedContentData = contentMap.leftOuterJoin(prevSideloadingSummary)
        joinedContentData.map { x => ContentSideloadingInput(x._1, x._2._1, x._2._2) }
    }

    override def algorithm(data: RDD[ContentSideloadingInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSideloadingOutput] = {

        val content_details = data.map { x => x.currentDetails }.flatMap { x => x }.map(x => (DeviceSummaryIndex(x.did, x.pdata.id, x.channel), Buffer(x)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b)
        val previous_device_details = content_details.map(x => x._1).joinWithCassandraTable[DeviceUsageSummary](Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE)
        val joinedData = content_details.leftOuterJoin(previous_device_details);
        val updatedAt = DateTime.now();
        val device_details = joinedData.map { y =>
            val previous_summary = y._2._2.getOrElse(DeviceUsageSummary(y._1.device_id, y._1.app_id, y._1.channel, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None));
            val num_contents = if (previous_summary.num_contents.isEmpty) y._2._1.size else (y._2._1.size + previous_summary.num_contents.get)
            (y._1.device_id, y._1.app_id, y._1.channel, Option(num_contents), updatedAt)
        }
        device_details.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_USAGE_SUMMARY_TABLE, SomeColumns("device_id", "app_id", "channel", "num_contents", "updated_date"))

        val download_details = content_details.flatMap { x => x._2 }.map { x =>
            val downloaded = if (x.transfer_count == 0) true else false
            val download_date = x.ts
            (x.did, x.content_id, x.pdata.id, x.channel, downloaded, download_date, DateTime.now())
        }
        download_details.saveToCassandra(Constants.DEVICE_KEY_SPACE_NAME, Constants.DEVICE_CONTENT_SUMMARY_FACT, SomeColumns("device_id", "content_id", "app_id", "channel", "downloaded", "download_date", "updated_date"))

        data.map { x =>
            val newContentMap = x.currentDetails
            val contentID = newContentMap.map(y => y.content_id).head
            val appId = x.index.appId
            val channelId = x.index.channel
            val prevContentSummary = x.previousDetails.getOrElse(ContentSideloading(contentID, appId, channelId, 0, 0, 0, Map[String, Double](), 0.0))
            val sortedTsList = newContentMap.map(y => y.ts).toList.sortBy { x => x }
            val syncts = newContentMap.last.syncts
            val dtRange = DtRange(sortedTsList.head, sortedTsList.last)
            val num_downloads = newContentMap.map { x => x.transfer_count }.toList.count(_ == 0.0) + prevContentSummary.num_downloads
            val total_count = newContentMap.map { y => y.did }.toList.size + prevContentSummary.total_count
            val num_sideloads = total_count - num_downloads
            //creating Map(origin,max(transfer_count))
            val currentOriginMap = x.currentDetails.groupBy { y => y.origin }.mapValues { z => z.map { x => x.transfer_count }.max }.map(f => (f._1, if (f._2 > prevContentSummary.origin_map.getOrElse(f._1, 0.0)) f._2 else prevContentSummary.origin_map.getOrElse(f._1, 0.0)))
            //Creating new origin map comparing current and previous maps
            val newOriginMap = prevContentSummary.origin_map ++ currentOriginMap
            val avgDepth = CommonUtil.roundDouble((newOriginMap.map { x => x._2 }.sum / newOriginMap.size), 2)

            ContentSideloadingOutput(ContentSideloading(contentID, appId, channelId, num_downloads, total_count, num_sideloads, newOriginMap, avgDepth), dtRange, syncts, newContentMap.head.pdata)
        }.cache();
    }

    override def postProcess(data: RDD[ContentSideloadingOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map(x => x.summary).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)

        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { csSummary =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_SIDELOADING_SUMMARY", null, "CUMULATIVE", DtRange(0l, 0l), csSummary.summary.content_id, Option(csSummary.summary.appId), Option(csSummary.summary.channel));
            val measures = Map(
                "num_downloads" -> csSummary.summary.num_downloads,
                "num_sideloads" -> csSummary.summary.num_sideloads,
                "avg_depth" -> csSummary.summary.avg_depth);
            MeasuredEvent("ME_CONTENT_SIDELOADING_SUMMARY", System.currentTimeMillis(), csSummary.synts, meEventVersion, mid, "", csSummary.summary.channel, Option(csSummary.summary.content_id), None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ContentSideloadingSummary").asInstanceOf[String])), None, "CUMULATIVE", csSummary.dtRange),
                Dimensions(None, None, Option(new GData(csSummary.summary.content_id, "")), None, None, None, Option(csSummary.pdata)),
                MEEdata(measures));
        }
    }
}