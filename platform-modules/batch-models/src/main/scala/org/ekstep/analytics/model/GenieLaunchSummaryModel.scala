package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.util.JobLogger
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

case class GenieSummary(did: String, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long,
                        tags: Option[AnyRef], dateRange: DtRange, stageSummary: Iterable[GenieStageSummary]) extends AlgoOutput
case class LaunchSessions(did: String, events: Buffer[Event]) extends AlgoInput
case class GenieStageSummary(stageId: String, sid: String, timeSpent: Double, visitCount: Int, interactEventsCount: Int, interactEvents: List[Map[String, String]])
case class StageDetails(timeSpent: Double, interactEvents: Buffer[Event], visitCount: Int, sid: String)

object GenieLaunchSummaryModel extends SessionBatchModel[Event, MeasuredEvent] with IBatchModelTemplate[Event, LaunchSessions, GenieSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieLaunchSummaryModel"
    override def name: String = "GenieLaunchSummaryModel"
    
    def computeGenieScreenSummary(events: Buffer[Event]): Iterable[GenieStageSummary] = {

        val screenInteractEvents = DataFilter.filter(events, Filter("eid", "IN", Option(List("GE_GENIE_START", "GE_INTERACT", "GE_GENIE_END"))))

        var stageMap = HashMap[String, StageDetails]();
        var screenSummaryList = Buffer[HashMap[String, Double]]();
        val screenInteractCount = DataFilter.filter(screenInteractEvents, Filter("eid", "EQ", Option("GE_INTERACT"))).length;
        if (screenInteractCount > 0) {
            var stageList = ListBuffer[(String, Double, Buffer[Event], String)]();
            var prevEvent = events(0);
            events.foreach { x =>
                x.eid match {
                    case "GE_GENIE_START" =>
                        stageList += Tuple4("splash", CommonUtil.getTimeDiff(prevEvent, x).get, Buffer[Event](), x.sid);
                    case "GE_INTERACT" =>
                        stageList += Tuple4(x.edata.eks.stageid, CommonUtil.getTimeDiff(prevEvent, x).get, Buffer(x), x.sid);
                    case "GE_GENIE_END" =>
                        stageList += Tuple4("endStage", CommonUtil.getTimeDiff(prevEvent, x).get, Buffer[Event](), x.sid);
                }
                prevEvent = x;
            }

            var currStage: String = null;
            var prevStage: String = null;
            stageList.foreach { x =>
                if (currStage == null) {
                    currStage = x._1;
                }
                if (stageMap.getOrElse(currStage, null) == null) {
                    stageMap.put(currStage, StageDetails(x._2, x._3, 0, x._4));
                } else {
                    stageMap.put(currStage, StageDetails(stageMap.get(currStage).get.timeSpent + x._2, stageMap.get(currStage).get.interactEvents ++ x._3, stageMap.get(currStage).get.visitCount, stageMap.get(currStage).get.sid));
                }
                if (currStage.equals(x._1)) {
                    if(prevStage != currStage)
                      stageMap.put(currStage, StageDetails(stageMap.get(currStage).get.timeSpent, stageMap.get(currStage).get.interactEvents, stageMap.get(currStage).get.visitCount + 1, stageMap.get(currStage).get.sid));
                    currStage = null;
                }
                prevStage = x._1;
            }
        }
        stageMap.map { x => 
            
            val interactEventsDetails = x._2.interactEvents.toList.map{ f => 
                Map("ID" -> f.edata.eks.id, "type" -> f.edata.eks.`type`, "subtype" -> f.edata.eks.subtype)
            }
            GenieStageSummary(x._1, x._2.sid, x._2.timeSpent, x._2.visitCount, x._2.interactEvents.length, interactEventsDetails)
        }
    }
    
    override def preProcess(data: RDD[Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LaunchSessions] = {
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);
        val genieLaunchSessions = getGenieLaunchSessions(data, idleTime);
        genieLaunchSessions.map { x => LaunchSessions(x._1, x._2) }
    }

    override def algorithm(data: RDD[LaunchSessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieSummary] = {

        data.map { x =>
            val geStart = x.events.head
            val geEnd = x.events.last
            val syncts = CommonUtil.getEventSyncTS(geEnd)
            val startTimestamp = CommonUtil.getEventTS(geStart)
            val endTimestamp = CommonUtil.getEventTS(geEnd)
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.events.filter { x => "OE_START".equals(x.eid) }.map { x => x.gdata.id }.filter { x => x != null }.distinct
            val stageSummary = computeGenieScreenSummary(x.events)
            GenieSummary(x.did, timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(geEnd.tags), dtRange, stageSummary);
        }.filter { x => (x.timeSpent >= 0) }
    }

    override def postProcess(data: RDD[GenieSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_LAUNCH_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange, summary.did);
            val measures = Map(
                "timeSpent" -> summary.timeSpent,
                "time_stamp" -> summary.time_stamp,
                "content" -> summary.content,
                "contentCount" -> summary.contentCount,
                "screenSummary" -> summary.stageSummary);
            MeasuredEvent("ME_GENIE_LAUNCH_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None),
                MEEdata(measures), summary.tags);
        }
    }
}