package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework._

case class WorkflowInput(sessionKey: String, events: Buffer[V3Event]) extends AlgoInput
case class WorkflowOutput(did: Option[String], sid: String, uid: String, pdata: PData, channel: String, content_id: Option[String], session_type: String, syncts: Long, dt_range: DtRange, mode: Option[String], item_responses: Option[Buffer[ItemResponse]],
                               start_time: Long, end_time: Long, time_spent: Double, time_diff: Double, interact_events_count: Long, interact_events_per_min: Double, telemetry_version: String,
                               env_summary: Option[Iterable[EnvSummary]], events_summary: Option[Iterable[EventSummary]],
                               page_summary: Option[Iterable[PageSummary]], etags: Option[ETags]) extends AlgoOutput

object WorkFlowSummaryModel extends IBatchModelTemplate[V3Event, WorkflowInput, WorkflowOutput, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.WorkFlowSummaryModel"
    override def name: String = "WorkFlowSummaryModel"
    val DEFAULT_MODE = "play";

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: V3Event): Item = {
        val item = itemMapping.getOrElse(event.edata.item.id, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }
    
    /**
     *
     */
    private def getItemData(contents: Array[Content], games: Array[String], apiVersion: String): Map[String, Item] = {

        val gameIds = contents.map { x => x.id };
        val codeIdMap: Map[String, String] = contents.map { x => (x.metadata.get("code").get.asInstanceOf[String], x.id) }.toMap;
        val contentItems = games.map { gameId =>
            {
                if (gameIds.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(gameId, apiVersion))
                } else if (codeIdMap.contains(gameId)) {
                    (gameId, ContentAdapter.getContentItems(codeIdMap.get(gameId).get, apiVersion))
                } else {
                    null;
                }
            }
        }.filter(x => x != null).filter(_._2 != null).toMap;

        if (contentItems.size > 0) {
            contentItems.map(f => {
                f._2.map { item =>
                    (item.id, item)
                }
            }).reduce((a, b) => a ++ b).toMap;
        } else {
            Map[String, Item]();
        }
    }

    private def arrangeWorkflowData(events: Buffer[V3Event]): Map[String, Buffer[V3Event]] = {

        val sortedEvents = events.sortBy { x => x.ets }
        var workFlowData = Map[String, Buffer[V3Event]]();
        var tmpArr = Buffer[V3Event]();
        val lastEventMid = events.last.mid

        var appKey = ""
        var sessionKey = ""
        var playerKey = ""
        var editorKey = ""

        sortedEvents.foreach { x =>
          
            val eventType = if(null == x.edata.`type`) "" else x.edata.`type`.toLowerCase();
            (x.eid, eventType) match {
                case ("START", "app") =>

                    if (appKey.isEmpty()) {
                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    } else if (workFlowData.contains(appKey)) {
                        // closing previous app workflow
                        workFlowData += (appKey -> (workFlowData.get(appKey).get ++ tmpArr))

                        // closing session workflow if any
                        if (sessionKey.nonEmpty && workFlowData.contains(sessionKey))
                            workFlowData += (sessionKey -> (workFlowData.get(sessionKey).get ++ tmpArr))
                        else if (sessionKey.nonEmpty && !workFlowData.contains(sessionKey))
                            workFlowData += (sessionKey -> tmpArr)
                        
                        // closing player workflow if any
                        if (playerKey.nonEmpty && !workFlowData.contains(playerKey))
                            workFlowData += (playerKey -> tmpArr)
                            
                        // closing editor workflow if any
                        if (editorKey.nonEmpty && !workFlowData.contains(editorKey))
                            workFlowData += (editorKey -> tmpArr)
                        
                        //adding new app-workflow
                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                        sessionKey = ""
                        playerKey = ""
                        editorKey = ""

                    } else {
                        // closing app workflow
                        workFlowData += (appKey -> tmpArr)
                        
                         // closing session workflow if any
                        if (sessionKey.nonEmpty && !workFlowData.contains(sessionKey)) 
                            workFlowData += (sessionKey -> tmpArr)
                        
                        // closing player workflow if any
                        if (playerKey.nonEmpty && !workFlowData.contains(playerKey))
                            workFlowData += (playerKey -> tmpArr)
                            
                        // closing editor workflow if any
                        if (editorKey.nonEmpty && !workFlowData.contains(editorKey))
                            workFlowData += (editorKey -> tmpArr)

                        appKey = "app" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                        sessionKey = ""
                        playerKey = ""
                        editorKey = ""
                    }

                case ("END", "app")       =>
                    tmpArr += x
                    // closing app workflow
                    workFlowData += (appKey -> (workFlowData.get(appKey).get ++ tmpArr))
                    tmpArr = Buffer[V3Event]();
                    
                case ("START", "session") =>
                    if (sessionKey.isEmpty()) {
                        sessionKey = "session" + x.ets
                        if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    } else {
                        // closing previous session workflow
                        workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        
                        // closing player workflow if any
                        if (playerKey.nonEmpty && !workFlowData.contains(playerKey))
                            workFlowData += (playerKey -> tmpArr)
                            
                        // closing editor workflow if any
                        if (editorKey.nonEmpty && !workFlowData.contains(editorKey))
                            workFlowData += (editorKey -> tmpArr)
                            
                        //adding new session-workflow
                        sessionKey = "session" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    }
                case ("END", "session") =>
                    tmpArr += x
                    // closing session workflow
                    workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    if(lastEventMid.equals(x.mid)) {
                      if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                      tmpArr = Buffer[V3Event]();
                    }
                    tmpArr = Buffer[V3Event]();

                case ("START", "player") =>
                    if (playerKey.isEmpty()) {
                       playerKey = "player" + x.ets
                       if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                       if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                       tmpArr = Buffer[V3Event]();
                       tmpArr += x;
                    } else {
                        // closing previous player workflow
                        workFlowData += (playerKey -> (workFlowData.get(playerKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        // Add player events to session & app
                        if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        //adding new player-workflow
                        playerKey = "player" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    }
                case ("END", "player")    =>
                    tmpArr += x
                    // closing player workflow
                    workFlowData += (playerKey -> (workFlowData.get(playerKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    // Add player events to session & app
                    if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    tmpArr = Buffer[V3Event]();

                case ("START", "editor") =>
                    if (editorKey.isEmpty()) {
                       editorKey = "editor" + x.ets
                       if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                       if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                       tmpArr = Buffer[V3Event]();
                       tmpArr += x;
                    } else {
                        // closing previous editor workflow
                        workFlowData += (editorKey -> (workFlowData.get(editorKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        // Add editor events to session & app
                        if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                        //adding new editor-workflow
                        editorKey = "editor" + x.ets
                        tmpArr = Buffer[V3Event]();
                        tmpArr += x;
                    }
                case ("END", "editor") =>
                    tmpArr += x
                    // closing editor workflow
                    workFlowData += (editorKey -> (workFlowData.get(editorKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    // Add editor events to session & app
                    if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                    tmpArr = Buffer[V3Event]();
                  
                case _ =>
                    tmpArr += x
                    if(lastEventMid.equals(x.mid)) {
                      if (appKey.nonEmpty) workFlowData += (appKey -> (workFlowData.get(appKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                      if (sessionKey.nonEmpty) workFlowData += (sessionKey -> (workFlowData.get(sessionKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                      // closing player workflow if any
                      if (playerKey.nonEmpty)
                          workFlowData += (playerKey -> (workFlowData.get(playerKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                            
                      // closing editor workflow if any
                      if (editorKey.nonEmpty)
                            workFlowData += (editorKey -> (workFlowData.get(editorKey).getOrElse(Buffer[V3Event]()) ++ tmpArr))
                      tmpArr = Buffer[V3Event]();
                    }
            }
        }
        workFlowData
    }

    override def preProcess(data: RDD[V3Event], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowInput] = {

        val deviceEvents = data.map { x => (x.context.did.getOrElse(""), Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => (x._1, x._2) }
            
        deviceEvents.map(f => arrangeWorkflowData(f._2)).map(x => (x.toBuffer)).flatMap(f => f).map(x => WorkflowInput(x._1, x._2));
    }
    override def algorithm(data: RDD[WorkflowInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[WorkflowOutput] = {
        
        val events = data.map { x => x.events }.flatMap { x => x }.filter(f => f.`object`.isDefined)
        val gameList = events.map { x => x.`object`.get.id }.distinct().collect();
        JobLogger.log("Fetching the Content and Item data from Learning Platform")
        val contents = ContentAdapter.getAllContent();
        val itemData = getItemData(contents, gameList, "v2");
        val itemMapping = sc.broadcast(itemData);
        
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        data.map { x =>
            val `type` = x.sessionKey.replaceAll("[^A-Za-z]+", "")
            val events = x.events
            val firstEvent = events.head;
            val lastEvent = events.last;
            val telemetryVer = firstEvent.ver;
            val startTimestamp = firstEvent.ets;
            val endTimestamp = lastEvent.ets;
            val pdata = CommonUtil.getAppDetails(firstEvent)
            val channelId = CommonUtil.getChannelId(firstEvent)
            val contentId = if(firstEvent.`object`.isDefined) firstEvent.`object`.get.id else null;
            val uid = if (lastEvent.actor.id.isEmpty()) "" else lastEvent.actor.id
            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);

            var tmpLastEvent: V3Event = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }
            var lastEventTs: Long = eventsWithTs.last._1.ets;
            var tempEvents = Buffer[(V3Event, Double)]();
            var eventsBuffer: Buffer[(V3Event, Double)] = Buffer();
            eventsWithTs.foreach { f =>
                f._1.eid match {
                    case "IMPRESSION" =>
                        if (tempEvents.isEmpty) {
                            tempEvents += f
                        } else if (lastEventTs == f._1.ets) {
                            val ts = tempEvents.map { x => x._2 }.sum
                            val tuple = (tempEvents.head._1, ts)
                            eventsBuffer += tuple
                            tempEvents = Buffer[(V3Event, Double)]();
                            tempEvents += f
                            val lastTs = tempEvents.map { x => x._2 }.sum
                            val lastTuple = (tempEvents.head._1, ts)
                            eventsBuffer += lastTuple
                        } else {
                            val ts = tempEvents.map { x => x._2 }.sum
                            val tuple = (tempEvents.head._1, ts)
                            eventsBuffer += tuple
                            tempEvents = Buffer[(V3Event, Double)]();
                            tempEvents += f
                        }
                    case _ =>
                        if (lastEventTs == f._1.ets && !tempEvents.isEmpty) {
                            val ts = tempEvents.map { x => x._2 }.sum
                            val tuple = (tempEvents.head._1, ts)
                            eventsBuffer += tuple
                        }
                        tempEvents += f
                }
            }
            // only for player session
            val itemResponses = if (StringUtils.startsWith(x.sessionKey, "player")) {
                val assessEvents = events.filter { x => "ASSESS".equals(x.eid) }.sortBy { x => x.ets };
                assessEvents.map { x =>
                    val itemObj = getItem(itemMapping.value, x);
                    val metadata = itemObj.metadata;
                    val resValues = if (null == x.edata.resvalues) Option(Array[Map[String, AnyRef]]().map(f => f.asInstanceOf[AnyRef])) else Option(x.edata.resvalues.map(f => f.asInstanceOf[AnyRef]))
                    val res = if (null == x.edata.resvalues) Option(Array[String]()); else Option(x.edata.resvalues.flatten.map { x => (x._1 + ":" + x._2.toString) });
                    val item = x.edata.item
                    ItemResponse(item.id, metadata.get("type"), metadata.get("qlevel"), Option(x.edata.duration), Option(Int.box(item.exlength)), res, resValues, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, Option(item.mmc), x.edata.score, x.ets, metadata.get("max_score"), metadata.get("domain"), x.edata.pass, Option(item.title), Option(item.desc));
                }
            } else null;
            val mode = if (StringUtils.startsWith(x.sessionKey, "player")) {
                if (null == firstEvent.edata.mode || firstEvent.edata.mode.isEmpty()) DEFAULT_MODE else firstEvent.edata.mode
            } else null;
            
              
            val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
            val interactEventsCount = events.filter { x => "INTERACT".equals(x.eid) }.size.toLong
            val interactEventsPerMin: Double = if (interactEventsCount == 0 || timeSpent == 0) 0d
            else if (timeSpent < 60.0) interactEventsCount.toDouble
            else BigDecimal(interactEventsCount / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;

            val eventSummaries = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));

            val impressionEventsWithTs = eventsBuffer.filter { x => "IMPRESSION".equals(x._1.eid)}.map(x => (x._1.edata.pageid, x))
            val pageSummaries = if (impressionEventsWithTs.length > 0) {
                impressionEventsWithTs.groupBy(f => f._1).map { f =>
                    val id = f._1
                    val firstEvent = f._2(0)._2._1
                    val `type` = firstEvent.edata.`type`
                    val env = firstEvent.context.env
                    val timeSpent = CommonUtil.roundDouble(f._2.map(x => x._2._2).sum, 2)
                    val visitCount = f._2.length.toLong
                    PageSummary(id, `type`, env, timeSpent, visitCount)
                }
            } else Iterable[PageSummary]();

            val envSummaries = if (pageSummaries.size > 0) {
                pageSummaries.groupBy { x => x.env }.map { f =>
                    val timeSpent = CommonUtil.roundDouble(f._2.map(x => x.time_spent).sum, 2)
                    val count = f._2.map(x => x.visit_count).max;
                    EnvSummary(f._1, timeSpent, count)
                }
            } else Iterable[EnvSummary]();

            WorkflowOutput(firstEvent.context.did, firstEvent.context.sid.getOrElse(""), uid, pdata, channelId, Option(contentId), `type`, CommonUtil.getEventSyncTS(lastEvent), DtRange(startTimestamp, endTimestamp), Option(mode), Option(itemResponses), startTimestamp, endTimestamp, timeSpent, timeDiff, interactEventsCount, interactEventsPerMin, telemetryVer, Option(envSummaries), Option(eventSummaries), Option(pageSummaries), Option(CommonUtil.getETags(firstEvent)))
        }.filter(f => (f.time_spent >= 1))
        
    }
    override def postProcess(data: RDD[WorkflowOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { session =>
            val mid = CommonUtil.getMessageId("ME_WORKFLOW_SUMMARY", session.uid, "SESSION", session.dt_range, "NA", Option(session.pdata.id), Option(session.channel));
            val measures = Map(
                "start_time" -> session.start_time,
                "end_time" -> session.end_time,
                "time_diff" -> session.time_diff,
                "time_spent" -> session.time_spent,
                "telemetry_version" -> session.telemetry_version,
                "mode" -> session.mode,
                "item_responses" -> session.item_responses,
                "interact_events_count" -> session.interact_events_count,
                "interact_events_per_min" -> session.interact_events_per_min,
                "env_summary" -> session.env_summary,
                "events_summary" -> session.events_summary,
                "page_summary" -> session.page_summary);
            MeasuredEvent("ME_WORKFLOW_SUMMARY", System.currentTimeMillis(), session.syncts, meEventVersion, mid, session.uid, null, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "WorkflowSummarizer").asInstanceOf[String])), None, "SESSION", session.dt_range),
                Dimensions(None, session.did, None, None, None, None, Option(PData(session.pdata.id, session.pdata.ver)), None, None, None, None, None, session.content_id, None, None, Option(session.sid), None, None, None, None, None, None, None, None, None, None, Option(session.channel), Option(session.session_type)),
                MEEdata(measures), session.etags);
        }
    }
}