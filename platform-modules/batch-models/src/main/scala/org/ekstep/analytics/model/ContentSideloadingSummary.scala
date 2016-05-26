package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.Event
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils

case class ContentSideloding(content_id: String, num_times_sideloaded: Double, num_devices: Long, avg_depth: Double)


object ContentSideloadingSummary extends IBatchModel[Event] with Serializable {
  
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
      
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("GE_TRANSFER")));
        val filteredEvents = DataFilter.filter(DataFilter.filter(events, Filter("edata.eks.direction", "EQ", Option("EXPORT"))), Filter("edata.eks.datatype", "EQ", Option("CONTENT")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        
        val reducedData = events.map{ event => 
            val contents = event.edata.eks.contents
            contents.map{ f => 
                (f.getOrElse("identifier", "").asInstanceOf[String],f.getOrElse("origin", "").asInstanceOf[String],f.getOrElse("transferCount", 0.0).asInstanceOf[Double],CommonUtil.getEventTS(event))
            }
        }.flatMap(f=>f)
        val contentOriginMap = reducedData.groupBy { x => (x._1,x._2) }.mapValues{ y => (y.map{z => z._3 }.toList.max , y.map{z=>z._4}.toList)}
        val contentMap = contentOriginMap.groupBy{x => x._1._1}
        
        val contentSideloadingSummary = contentMap.mapValues{ x => 
            
            val sortedTsList = x.map(y => y._2._2).flatMap { x => x }.toList.sortBy { x => x }
            val dtRange = DtRange(sortedTsList.head,sortedTsList.last)
            val contentID = x.map(y => y._1._1).head
            val numTimesSideloaded = x.map{y => y._2._1}.sum
            val numDevices = x.map{y => y._1._2}.toList.distinct.size
            val avgDepth:Double = CommonUtil.roundDouble((numTimesSideloaded/numDevices),2)
            
            (ContentSideloding(contentID,numTimesSideloaded,numDevices,avgDepth),dtRange)
        }
      
        contentSideloadingSummary.map { f =>
            getMeasuredEvent(f._2._1, configMapping.value, f._2._2);
        }.map { x => JSONUtils.serialize(x) };
      
   }
    
   private def getMeasuredEvent(sideloadingSummary:ContentSideloding , config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {

        val mid = CommonUtil.getMessageId("ME_CONTENT_SIDELOADING_SUMMARY", sideloadingSummary.content_id, null, DtRange(0l,0l));
        val measures = Map(
            "num_times_sideloaded" -> sideloadingSummary.num_times_sideloaded,
            "num_devices" -> sideloadingSummary.num_devices,
            "avg_depth" -> sideloadingSummary.avg_depth);
        MeasuredEvent("ME_CONTENT_SIDELOADING_SUMMARY", System.currentTimeMillis(), dtRange.to, "1.0", mid, None, Option(sideloadingSummary.content_id), None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSideloadingSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "CUMULATIVE", dtRange),
            Dimensions(None, None, None, None, None, None, None, None, None),
            MEEdata(measures));
    }
}