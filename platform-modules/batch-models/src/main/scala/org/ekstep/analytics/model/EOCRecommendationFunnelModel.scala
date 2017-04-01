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

case class EOCFunnel(uid: String, did: String, dtRange: DtRange, consumed: Int, contentShown: List[AnyRef], contentCount: Int, downloadInit: Int, downloadComplete: Int, played: Int) extends AlgoOutput
case class EventsGroup(uid: String, did: String, events: List[Event]) extends AlgoInput
object EOCRecommendationFunnelModel extends IBatchModelTemplate[Event, EventsGroup, EOCFunnel, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.EOCRecommendationFunnelModel"
    override def name: String = "EOCRecommendationFunnelModel"

    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EventsGroup] = {
        println("Preprocessing start.......")
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);
        val queries = Option(Array(
            Query(Option("ekstep-dev-data-store"), Option("raw/"), Option("2017-01-01"), Option("2017-03-25"))));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
        val rdd1 = DataFilter.filter(rdd, Array(Filter("eid", "EQ", Option("OE_INTERACT")), Filter("edata.eks.id", "EQ", Option("gc_relatedcontent"))))
        //rdd1.foreach { x => println(JSONUtils.serialize(x)) }
        val rdd2 = rdd.filter { x => (x.eid.equals("OE_START") || x.eid.equals("OE_END") || x.eid.equals("GE_INTERACT") || x.eid.equals("GE_SERVICE_API_CALL")) }
        val rdd3 = rdd1.union(rdd2)
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
            val dtRange = DtRange(startTimestamp, endTimestamp);
            var value = 0
            var consumed = 0
            var contentList = List[String]()
            var contentCount = 0
            var downloadInit = 0
            var downloadComplete = 0
            var played = 0
            var playedContentId = ""
            var eofFunnel = EOCFunnel(x.uid, x.did, dtRange, 0, List(), 0, 0, 0, 0)

            x.events.foreach { x =>
                if (x.eid.equals("OE_START")) {
                    value = 1
                    if (x.gdata.id.equals(playedContentId)) {
                        played = 1
                    } else {
                        played = 0
                    }
                }

                if (value == 1) {

                    x.eid match {

                        case "GE_SERVICE_API_CALL" =>
                            consumed = 1

                        case "OE_INTERACT" =>

                            val contentMap = x.edata.eks.values
                            if (contentMap.isEmpty) {
                                contentList = List()
                                contentCount = 0
                            } else {
                                contentList = contentMap(1).asInstanceOf[Map[String, List[String]]].getOrElse("ContentIDsDisplayed", List())

                                contentCount = contentList.size
                                val positionClicked = contentMap(0).asInstanceOf[Map[String, Double]].getOrElse("PositionClicked", 0.0).toInt

                                playedContentId = contentList(positionClicked - 1)
                            }

                        case "GE_INTERACT" =>

                            if (x.edata.eks.subtype.equals("ContentDownload-Initiate")) {

                                downloadInit = 1
                            }
                            if (x.edata.eks.subtype.equals("ContentDownload-Success")) {
                                downloadComplete = 1
                            }

                        case "OE_END" =>

                            eofFunnel = EOCFunnel(x.uid, x.did, dtRange, consumed, contentList, contentCount, downloadInit, downloadComplete, played)
                            value = 0
                            consumed = 0
                            contentList = List[String]()
                            contentCount = 0
                            downloadInit = 0
                            downloadComplete = 0

                        case _ =>

                    }
                }

            }
            eofFunnel

        }

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
        

    
