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

case class ContentSideloading(content_id: String, num_downloads: Long, total_count: Long, num_sideloads: Long, origin_map: Map[String, Double], avg_depth: Double)
case class ReducedContentDetails(content_id: String, transfer_count: Double, did: String, origin: String, ts: Long)

object ContentSideloadingSummary extends IBatchModel[Event] with Serializable {

    val className = "org.ekstep.analytics.model.ContentSideloadingSummary"
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.debug("### Execute method started ###", className);
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_TRANSFER")));
        val filteredEvents = DataFilter.filter(DataFilter.filter(events, Filter("edata.eks.direction", "EQ", Option("IMPORT"))), Filter("edata.eks.datatype", "EQ", Option("CONTENT")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val reducedData = filteredEvents.map { event =>
            val contents = event.edata.eks.contents
            contents.map { f =>
                ReducedContentDetails(f.get("identifier").get.asInstanceOf[String], f.get("transferCount").get.asInstanceOf[Double], event.did, f.get("origin").get.asInstanceOf[String], CommonUtil.getEventTS(event))
            }
        }.flatMap(f => f)
        val contentMap = reducedData.groupBy { x => x.content_id }.partitionBy(new HashPartitioner(JobContext.parallelization))
        val prevSideloadingSummary = contentMap.map(f => ContentId(f._1)).joinWithCassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY).map(f => (f._1.content_id, f._2))
        val joinedContentData = contentMap.leftOuterJoin(prevSideloadingSummary)

        val contentSideloadingSummary = joinedContentData.mapValues { x =>

            val newContentMap = x._1
            val contentID = newContentMap.map(y => y.content_id).head
            val prevContentSummary = x._2.getOrElse(ContentSideloading(contentID, 0, 0, 0, Map[String, Double](), 0.0))
            val sortedTsList = newContentMap.map(y => y.ts).toList.sortBy { x => x }
            val dtRange = DtRange(sortedTsList.head, sortedTsList.last)
            val num_downloads = newContentMap.map { x => x.transfer_count }.toList.count(_ == 0.0) + prevContentSummary.num_downloads
            val total_count = newContentMap.map { y => y.did }.toList.size + prevContentSummary.total_count
            val num_sideloads = total_count - num_downloads
            //creating Map(origin,max(transfer_count))
            val currentOriginMap = x._1.groupBy { y => y.origin }.mapValues { z => z.map { x => x.transfer_count }.max }.map(f => (f._1, if(f._2 > prevContentSummary.origin_map.getOrElse(f._1, 0.0)) f._2 else prevContentSummary.origin_map.getOrElse(f._1, 0.0)) )
            //Creating new origin map comparing current and previous maps
            val newOriginMap = prevContentSummary.origin_map ++ currentOriginMap
            val avgDepth = CommonUtil.roundDouble((newOriginMap.map { x => x._2 }.sum / newOriginMap.size), 2)

            (ContentSideloading(contentID, num_downloads, total_count, num_sideloads, newOriginMap, avgDepth), dtRange)
        }.cache();

        contentSideloadingSummary.map(x => x._2._1).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)
        
        JobLogger.debug("### Execute method ended ###", className);
        contentSideloadingSummary.map { f =>
            getMeasuredEvent(f._2._1, configMapping.value, f._2._2);
        }.map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEvent(sideloadingSummary: ContentSideloading, config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {

        val mid = CommonUtil.getMessageId("ME_CONTENT_SIDELOADING_SUMMARY", sideloadingSummary.content_id, null, DtRange(0l, 0l));
        val measures = Map(
            "num_downloads" -> sideloadingSummary.num_downloads,
            "num_sideloads" -> sideloadingSummary.num_sideloads,
            "avg_depth" -> sideloadingSummary.avg_depth);
        MeasuredEvent("ME_CONTENT_SIDELOADING_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, "", Option(sideloadingSummary.content_id), None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSideloadingSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "CUMULATIVE", dtRange),
            Dimensions(None, None, None, None, None, None, None, None, None),
            MEEdata(measures));
    }
}