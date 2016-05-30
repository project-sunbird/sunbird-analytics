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

case class ContentSideloading(content_id: String, num_downloads: Long, total_count:Long, num_sideloads: Long)

object ContentSideloadingSummary extends IBatchModel[Event] with Serializable {
  
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
      
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_TRANSFER")));
        val filteredEvents = DataFilter.filter(DataFilter.filter(events, Filter("edata.eks.direction", "EQ", Option("IMPORT"))), Filter("edata.eks.datatype", "EQ", Option("CONTENT")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        
        val reducedData = filteredEvents.map{ event => 
            val contents = event.edata.eks.contents
            contents.map{ f => 
                (f.get("identifier").get.asInstanceOf[String],f.get("transferCount").get.asInstanceOf[Double],event.did,CommonUtil.getEventTS(event))
            }
        }.flatMap(f=>f)
        val contentMap = reducedData.groupBy{x => x._1}.partitionBy(new HashPartitioner(JobContext.parallelization))
        val prevSideloadingSummary = contentMap.map(f => ContentId(f._1)).joinWithCassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY).map(f => (f._1.content_id, f._2))
        val joinedContentData = contentMap.leftOuterJoin(prevSideloadingSummary)
        
        val contentSideloadingSummary = joinedContentData.mapValues{ x => 
            
            val newContentMap = x._1
            val contentID = newContentMap.map(y => y._1).head
            val prevContentSummary = x._2.getOrElse(ContentSideloading(contentID,0,0,0))
            val sortedTsList = newContentMap.map(y => y._4).toList.sortBy { x => x }
            val dtRange = DtRange(sortedTsList.head,sortedTsList.last)
            val num_downloads = newContentMap.map{x => x._2}.toList.count(_==0.0) + prevContentSummary.num_downloads
            val total_count = newContentMap.map{y => y._3}.toList.size + prevContentSummary.total_count
            val num_sideloads = total_count - num_downloads
            
            (ContentSideloading(contentID,num_downloads,total_count,num_sideloads),dtRange)
        }
      
        contentSideloadingSummary.map(x => x._2._1).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)
        
        contentSideloadingSummary.map { f =>
            getMeasuredEvent(f._2._1, configMapping.value, f._2._2);
        }.map { x => JSONUtils.serialize(x) };
   }
    
   private def getMeasuredEvent(sideloadingSummary:ContentSideloading , config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {

        val mid = CommonUtil.getMessageId("ME_CONTENT_SIDELOADING_SUMMARY", sideloadingSummary.content_id, null, DtRange(0l,0l));
        val measures = Map(
            "num_downloads" -> sideloadingSummary.num_downloads,
            "num_sideloads" -> sideloadingSummary.num_sideloads);
        MeasuredEvent("ME_CONTENT_SIDELOADING_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, Option(sideloadingSummary.content_id), None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSideloadingSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "CUMULATIVE", dtRange),
            Dimensions(None, None, None, None, None, None, None, None, None),
            MEEdata(measures));
    }
}