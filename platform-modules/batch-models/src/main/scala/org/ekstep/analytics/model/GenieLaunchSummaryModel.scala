package org.ekstep.analytics.model

import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.ETags
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.exception.DataFilterException
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.SessionBatchModel
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.V3Event
import org.ekstep.analytics.util.Constants
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.V3PData

case class GenieSummary(did: String, timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int, syncts: Long,
                        etags: Option[ETags] = Option(ETags(None, None, None)), dateRange: DtRange, stageSummary: Iterable[GenieStageSummary], pdata: PData, channel: String) extends AlgoOutput
case class LaunchSessions(channel: String, did: String, events: Buffer[V3Event]) extends AlgoInput
case class GenieStageSummary(stageId: String, sid: String, timeSpent: Double, visitCount: Int, interactEventsCount: Int, interactEvents: List[Map[String, String]])
case class StageDetails(timeSpent: Double, interactEvents: Buffer[V3Event], visitCount: Int, sid: String)

object GenieLaunchSummaryModel extends SessionBatchModel[V3Event, MeasuredEvent] with IBatchModelTemplate[V3Event, LaunchSessions, GenieSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.GenieLaunchSummaryModel"
    override def name: String = "GenieLaunchSummaryModel"

    def computeGenieScreenSummary(events: Buffer[V3Event]): Iterable[GenieStageSummary] = {
        val genieEvents = DataFilter.filter(events, Filter("context.env", "IN", Option(List(Constants.GENIE_ENV))))
        val screenInteractEvents = DataFilter.filter(genieEvents, Filter("eid", "IN", Option(List("START", "IMPRESSION", "END"))))

        var stageMap = HashMap[String, StageDetails]();
        var screenSummaryList = Buffer[HashMap[String, Double]]();
        val screenInteractCount = DataFilter.filter(screenInteractEvents, Filter("eid", "EQ", Option("IMPRESSION"))).length;
        if (screenInteractCount > 0) {
            var stageList = ListBuffer[(String, Double, Buffer[V3Event], String)]();
            var prevEvent = events(0);
            screenInteractEvents.foreach { x =>
                x.eid match {
                    case "START" =>
                        stageList += Tuple4("splash", CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get, Buffer[V3Event](), x.context.sid.get);
                    case "IMPRESSION" | "INTERACT" =>
                        stageList += Tuple4(x.edata.pageid, CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get, Buffer(x), x.context.sid.get);
                    case "END" =>
                        stageList += Tuple4("endStage", CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get, Buffer[V3Event](), x.context.sid.get);
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
                    if (prevStage != currStage)
                        stageMap.put(currStage, StageDetails(stageMap.get(currStage).get.timeSpent, stageMap.get(currStage).get.interactEvents, stageMap.get(currStage).get.visitCount + 1, stageMap.get(currStage).get.sid));
                    currStage = null;
                }
                prevStage = x._1;
            }
        }
        stageMap.map { x =>

            val interactEventsDetails = x._2.interactEvents.toList.map { f =>
                Map("ID" -> f.edata.id, "type" -> f.edata.`type`, "subtype" -> f.edata.subtype)
            }
            GenieStageSummary(x._1, x._2.sid, x._2.timeSpent, x._2.visitCount, x._2.interactEvents.length, interactEventsDetails)
        }
    }

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LaunchSessions] = {
        val envList = List(Constants.GENIE_ENV, Constants.PLAYER_ENV)
        val env = sc.broadcast(envList);
        

        //val events = DataFilter.filter(data, Filter("context.env", "IN", Option(env))) // deprecated 
        /** Filter criteria **/
        // pdata.pid=genieservice.android or context.env=Genie or context.env=ContentPlayer 
        val events = data.filter { x => env.value.contains(x.context.env) || StringUtils.equals("genieservice.android", x.context.pdata.getOrElse(V3PData("")).pid.getOrElse(""))}
        
        val idleTime = config.getOrElse("idleTime", 30).asInstanceOf[Int]
        val jobConfig = sc.broadcast(config);
        val filteredData = data.filter { x => !("AutoSync-Initiated".equals(x.edata.subtype) || "AutoSync-Success".equals(x.edata.subtype)) } // Change to edata.subtype
        val genieLaunchSessions = getV3GenieLaunchSessions(filteredData, idleTime);
        genieLaunchSessions.map { x => LaunchSessions(x._1._1, x._1._2, x._2) }
    }

    override def algorithm(data: RDD[LaunchSessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GenieSummary] = {

        data.map { x =>
            val geStart = x.events.head
            val geEnd = x.events.last

            val pdata = CommonUtil.getAppDetails(geStart)
            val channel = x.channel

            val syncts = CommonUtil.getEventSyncTS(geEnd)
            val startTimestamp = geStart.ets
            val endTimestamp = geEnd.ets
            val dtRange = DtRange(startTimestamp, endTimestamp);
            val timeSpent = CommonUtil.getTimeDiff(startTimestamp, endTimestamp)
            val content = x.events.filter { x => "START".equals(x.eid) && Constants.PLAYER_ENV.equals(x.context.env) }.map { x => x.`object`.get.id }.filter { x => x != null }.distinct
            val stageSummary = computeGenieScreenSummary(x.events)
            GenieSummary(x.did, timeSpent.getOrElse(0d), endTimestamp, content, content.size, syncts, Option(CommonUtil.getETags(geEnd)), dtRange, stageSummary, pdata, channel);
        }.filter { x => (x.timeSpent >= 0) }
    }

    override def postProcess(data: RDD[GenieSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_LAUNCH_SUMMARY", null, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange, summary.did, Option(summary.pdata.id), Option(summary.channel));
            val measures = Map(
                "timeSpent" -> summary.timeSpent,
                "time_stamp" -> summary.time_stamp,
                "content" -> summary.content,
                "contentCount" -> summary.contentCount,
                "screenSummary" -> summary.stageSummary);
            MeasuredEvent("ME_GENIE_LAUNCH_SUMMARY", System.currentTimeMillis(), summary.syncts, meEventVersion, mid, "", summary.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "GenieUsageSummarizer").asInstanceOf[String])), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, Option(summary.pdata)),
                MEEdata(measures), summary.etags);
        }
    }
}