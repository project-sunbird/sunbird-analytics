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

case class EOCFunnel(uid: String, did: String, dtRange: DtRange, consumed: Int, contentShown: List[AnyRef], contentCount: Int, downloadInit: Int, downloadComplete: Int, played: Int) extends AlgoOutput
case class EventsGroup(uid: String, did: String, events: List[Event]) extends AlgoInput
object EOCRecommendationFunnelModel extends IBatchModelTemplate[Event, EventsGroup, EOCFunnel, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.EOCRecommendationFunnelModel"
    override def name: String = "EOCRecommendationFunnelModel"

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EventsGroup] = {
        val rdd = DataFilter.filter(data, Array(Filter("eid", "EQ", Option("GE_SERVICE_API_CALL")), Filter("edata.eks.method", "EQ", Option("getRelatedContent"))))
        val rdd1 = DataFilter.filter(data, Array(Filter("eid", "EQ", Option("OE_INTERACT")), Filter("edata.eks.id", "EQ", Option("gc_relatedcontent"))))
        val rdd2 = data.filter { x => (x.eid.equals("OE_START") || x.eid.equals("OE_END") || x.eid.equals("GE_INTERACT")) }
        val rdd3 = rdd.union(rdd1).union(rdd2)
        val rdd4 = rdd3.groupBy { x => (x.did, x.uid) }
        val rdd5 = rdd4.map { x => (x._1, x._2.toList.sortBy { x => x.`@timestamp` }) }.map { case ((did, uid), list) => (did, uid, list) }
        rdd5.map { x => EventsGroup(x._1, x._2, x._3) }

    }

    override def algorithm(data: RDD[EventsGroup], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EOCFunnel] = {

        data.map { x =>
            val geStart = x.events.head
            val geEnd = x.events.last
            val syncts = CommonUtil.getEventSyncTS(geEnd)
            val startTimestamp = CommonUtil.getEventTS(geStart)
            val endTimestamp = CommonUtil.getEventTS(geEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp)
            var consumed = 0
            var oeStart = 0
            var contentShown = List[String]()
            var contentCount = 0
            var downloadInit = 0
            var downloadComplete = 0
            var playedContentId = ""
            var oeEnd = 0
            var list = Buffer[EOCFunnel]()
            var oe_EndCount = 0
            val lastOccuranceOE_END = x.events.filter { x => x.eid.equals("OE_END") }.size
            x.events.map { x =>
                if (x.eid.equals("OE_START")) {
                    oeStart = 1
                    val played = if (x.gdata.id.equals(playedContentId)) 1 else 0

                    if (oeEnd == 1) {
                        list += EOCFunnel(x.uid, x.did, dtRange, consumed, contentShown, contentCount, downloadInit, downloadComplete, played)
                        consumed = 0
                        contentShown = List[String]()
                        contentCount = 0
                        downloadInit = 0
                        downloadComplete = 0
                        oeEnd = 0
                    }
                }
                if (oeStart == 1) {

                    x.eid match {

                        case "OE_INTERACT" =>
                            consumed = 1
                            val contentMap = x.edata.eks.values
                            if (contentMap.nonEmpty) {
                                val dispMap = contentMap.filter { x =>
                                    val map = x.asInstanceOf[Map[String, AnyRef]]
                                    map.contains("ContentIDsDisplayed")
                                }
                                contentShown = if (dispMap.isEmpty) List() else dispMap.last.asInstanceOf[Map[String, List[String]]].get("ContentIDsDisplayed").get
                                val pos = contentMap.filter { x =>
                                    val map = x.asInstanceOf[Map[String, AnyRef]]
                                    map.contains("PositionClicked")
                                }
                                val positionClicked = pos.last.asInstanceOf[Map[String, Double]].get("PositionClicked").get.toInt
                                playedContentId = if (contentShown.isEmpty) "" else contentShown(positionClicked - 1)
                            }

                            contentCount = if (contentMap.isEmpty) 0 else contentShown.size

                        case "GE_INTERACT" =>
                            if (x.edata.eks.subtype.equals("ContentDownload-Initiate")) {
                                downloadInit = 1
                            }
                            if (x.edata.eks.subtype.equals("ContentDownload-Success")) {
                                downloadComplete = 1
                            }

                        case "OE_END" =>
                            oe_EndCount += 1
                            if (lastOccuranceOE_END == oe_EndCount) {
                                list += EOCFunnel(x.uid, x.did, dtRange, consumed, contentShown, contentCount, downloadInit, downloadComplete, 0)
                                oe_EndCount = 0
                            }
                            oeStart = 0
                            oeEnd = 1

                        case _ =>

                    }
                }
                list
            }.last

        }.flatMap { x => x }

    }

    override def postProcess(data: RDD[EOCFunnel], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME__EOC_RECOMMENDATION_FUNNEL", null, config.getOrElse("granularity", "EVENT").asInstanceOf[String], summary.dtRange, summary.did);
            val measures = Map(
                "consumed" -> summary.consumed,
                "contentShown" -> summary.contentShown,
                "contentCount" -> summary.contentCount,
                "downloadInit" -> summary.downloadInit,
                "downloadComplete" -> summary.downloadComplete,
                "played" -> summary.played)

            MeasuredEvent("ME__EOC_RECOMMENDATION_FUNNEL", System.currentTimeMillis(), 0L, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "EOCRecommendationFunnelSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "EVENT").asInstanceOf[String], summary.dtRange),
                Dimensions(Option(summary.uid), Option(summary.did), None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }

    }
}

