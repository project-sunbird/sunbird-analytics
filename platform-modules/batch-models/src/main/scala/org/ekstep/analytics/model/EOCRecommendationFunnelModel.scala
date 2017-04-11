package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.Buffer

case class EOCFunnel(var uid: String,var did: String,var sid: String,var contentId: String, var contentViewed: String, var startDate: Long,var endDate: Long, var syncts: Long, var consumed: Int,var contentShown: List[AnyRef],var contentCount: Int,var downloadInit: Int,var downloadComplete: Int,var played: Int) extends AlgoOutput
case class EventsGroup(uid: String, did: String, sid: String, events: Buffer[Event]) extends AlgoInput
object EOCRecommendationFunnelModel extends IBatchModelTemplate[Event, EventsGroup, EOCFunnel, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.EOCRecommendationFunnelModel"
    override def name: String = "EOCRecommendationFunnelModel"

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EventsGroup] = {
        
        val rdd = DataFilter.filter(data, Array(Filter("eid", "EQ", Option("GE_SERVICE_API_CALL")), Filter("edata.eks.method", "EQ", Option("getRelatedContent"))));
        val rdd1 = DataFilter.filter(data, Array(Filter("eid", "EQ", Option("OE_INTERACT")), Filter("edata.eks.id", "EQ", Option("gc_relatedcontent"))))
        val rdd2 = data.filter { x => (x.eid.equals("OE_START") || x.eid.equals("OE_END") || x.eid.equals("GE_INTERACT")) }
        val rdd3 = rdd.union(rdd1).union(rdd2)
        val rdd4 = rdd3.groupBy { x => (x.did, x.uid, x.sid) }
        val rdd5 = rdd4.map { x => (x._1, x._2.toList.sortBy { x => x.`@timestamp` }) }.map { case ((did, uid, sid), list) => (did, uid, sid, list) }
        rdd5.map { x => EventsGroup(x._1, x._2, x._3, x._4.toBuffer) }
    }

    override def algorithm(data: RDD[EventsGroup], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EOCFunnel] = {

        data.map { group =>
            
            val geServiceEvents = DataFilter.filter(group.events, Filter("eid", "EQ", Option("GE_SERVICE_API_CALL")));
             
            var funnels = Buffer[EOCFunnel]();
            if(geServiceEvents.length > 0) {
                var funnel: EOCFunnel = null;
                group.events.foreach { event => 
                    event.eid match {
                        case "GE_SERVICE_API_CALL" =>
                            if(null != funnel) {
                                funnel.endDate = event.ets;
                                funnel.syncts = CommonUtil.getEventSyncTS(event);
                                funnels += funnel;
                                funnel = null;
                            }
                            val contentId = event.edata.eks.request.asInstanceOf[Map[String, AnyRef]].get("params");
                            if(contentId.isDefined) {
                                funnel = EOCFunnel(group.uid, group.did, group.sid, contentId.get.asInstanceOf[String], "", event.ets, 0L, CommonUtil.getEventSyncTS(event), 0, List(), 0, 0, 0, 0);   
                            }
                        case "OE_INTERACT" =>
                            if (null != funnel) {
                                if(!event.gdata.id.equals(funnel.contentId)) {
                                    funnel.endDate = event.ets;
                                    funnel.syncts = CommonUtil.getEventSyncTS(event);
                                    funnels += funnel;
                                    funnel = null;
                                } else {
                                    val contentMap = event.edata.eks.values
                                    if (contentMap.nonEmpty) {
                                        val valueMap = contentMap.map { x => x.asInstanceOf[Map[String, AnyRef]] }.filter { x =>
                                            x.contains("ContentIDsDisplayed") || x.contains("PositionClicked")
                                        }
                                        if (!valueMap.isEmpty) {
                                            funnel.consumed = 1;
                                            val contentShown = valueMap.filter(p => p.contains("ContentIDsDisplayed"));
                                            if(!contentShown.isEmpty) {
                                                funnel.contentShown = contentShown.head.get("ContentIDsDisplayed").get.asInstanceOf[List[String]];
                                                funnel.contentCount = funnel.contentShown.length;
                                            }
                                            val positionClicked = valueMap.filter(p => p.contains("PositionClicked"));
                                            if(!positionClicked.isEmpty) {
                                                val pos = positionClicked.head.get("PositionClicked").get.asInstanceOf[Double].toInt;
                                                if(!funnel.contentShown.isEmpty && funnel.contentShown.length >= pos) {
                                                    funnel.contentViewed = funnel.contentShown(pos-1).asInstanceOf[String];
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        case "GE_INTERACT" =>
                            if (null != funnel && funnel.consumed == 1 && funnel.contentViewed.equals(event.edata.eks.id)) {
                                if("ContentDownload-Initiate".equals(event.edata.eks.subtype)) funnel.downloadInit = 1;
                                if("ContentDownload-Success".equals(event.edata.eks.subtype)) funnel.downloadComplete = 1;
                            }
                        case "OE_START" =>
                            if(null != funnel) {
                                if(funnel.consumed == 1 && funnel.contentViewed.equals(event.gdata.id)) funnel.played = 1;
                                funnel.endDate = event.ets;
                                funnel.syncts = CommonUtil.getEventSyncTS(event);
                                funnels += funnel;
                                funnel = null;
                            }
                        case _ =>
                    }
                }
                if(null != funnel) {
                    funnel.endDate = funnel.startDate;
                    funnels += funnel;
                }
            }
            funnels
        }.flatMap { x => x }

    }

    override def postProcess(data: RDD[EOCFunnel], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val dtRange = DtRange(summary.startDate, summary.endDate);
            val mid = CommonUtil.getMessageId("ME_EOC_RECOMMENDATION_FUNNEL", summary.uid, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], dtRange, summary.did + summary.sid + summary.contentId);
            val measures = Map(
                "consumed" -> summary.consumed,
                "content_viewed" -> summary.contentViewed,
                "content_recommended" -> summary.contentShown,
                "content_count" -> summary.contentCount,
                "download_initiated" -> summary.downloadInit,
                "download_complete" -> summary.downloadComplete,
                "content_played" -> summary.played)

            MeasuredEvent("ME_EOC_RECOMMENDATION_FUNNEL", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.uid, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "EOCRecommendationFunnelSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "EVENT").asInstanceOf[String], dtRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None, None, None, Option(summary.contentId)),
                MEEdata(measures), None);
        }

    }
}

