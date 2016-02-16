package org.ekstep.analytics.model

import scala.BigDecimal
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.adapter._
import org.ekstep.analytics.framework.util._

/**
 * Case class to hold the item responses
 */
case class ItemResponse(itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Array[String], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Int, time_stamp: Option[Long], maxScore: Option[AnyRef], domain: Option[AnyRef]);

case class ActivitySummary(count: Int, timeSpent: Double)

/**
 * Case class to hold the screener summary
 */
class SessionSummary(
    val id: String, val ver: String, val levels: Option[Array[Map[String, Any]]], val noOfAttempts: Int,
    val timeSpent: Double, val interruptTime: Double, val timeDiff: Double, val start_time: Option[Long], val end_time: Option[Long], val currentLevel: Option[Map[String, String]],
    val noOfLevelTransitions: Option[Int], val comments: Option[String], val fluency: Option[Int], val loc: Option[String],
    val itemResponses: Option[Buffer[ItemResponse]], val dtRange: DtRange, val interactEventsPerMin: Double, val activitySummary: Option[Map[String, ActivitySummary]],
    val completionStatus: Option[Boolean], val screenSummary: Option[Map[String, Double]], val noOfInteractEvents: Int, val eventsSummary: Map[String, Int],
    val syncDate: Long) {};

/**
 * @author Santhosh
 */

/**
 * Generic Screener Summary Model
 */
object LearnerSessionSummary extends SessionBatchModel[Event] with Serializable {

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: Event): Item = {
        val item = itemMapping.getOrElse(event.edata.eks.qid, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItemDomain(itemMapping: Map[String, Item], event: Event): String = {
        val item = itemMapping.getOrElse(event.edata.eks.qid, null);
        if (null != item) {
            return item.metadata.getOrElse("domain", "").asInstanceOf[String];
        }
        return "";
    }

    /**
     *
     */
    private def getItemData(games: Array[String], apiVersion: String): Map[String, Item] = {

        val lpGameList = ContentAdapter.getGameList();
        val gameIds = lpGameList.map { x => x.identifier };
        val codeIdMap: Map[String, String] = lpGameList.map { x => (x.code, x.identifier) }.toMap;
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

    /**
     * Compute screen summaries on the telemetry data produced by content app
     */
    def computeScreenSummary(events: Buffer[Event]): Map[String, Double] = {

        val screenInteractEvents = DataFilter.filter(events, Filter("eid", "NIN", Option(List("OE_ASSESS", "OE_LEVEL_SET")))).filter { event =>
            event.eid match {
                case "OE_INTERACT" =>
                    event.edata.ext != null && event.edata.ext.stageId != null;
                case _ =>
                    true
            }
        }
        var stageMap = HashMap[String, Double]();
        val screenInteractCount = DataFilter.filter(screenInteractEvents, Filter("eid", "EQ", Option("OE_INTERACT"))).length;
        if (screenInteractCount > 0) {
            var stageList = ListBuffer[(String, Double)]();
            var prevEvent = events(0);
            screenInteractEvents.foreach { x =>
                x.eid match {
                    case "OE_START" =>
                        stageList += Tuple2("splash", CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_INTERACT" =>
                        stageList += Tuple2(x.edata.ext.stageId, CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_INTERRUPT" =>
                        stageList += Tuple2(x.edata.eks.id, CommonUtil.getTimeDiff(prevEvent, x).get);
                    case "OE_END" =>
                        stageList += Tuple2("endStage", CommonUtil.getTimeDiff(prevEvent, x).get);
                }
                prevEvent = x;
            }

            var currStage: String = null;
            stageList.foreach { x =>
                if (currStage == null) {
                    currStage = x._1;
                }
                if (stageMap.getOrElse(currStage, null) == null) {
                    stageMap.put(currStage, x._2);
                } else {
                    stageMap.put(currStage, stageMap.get(currStage).get + x._2);
                }
                if (!currStage.equals(x._1)) {
                    currStage = x._1;
                }
            }
        }

        stageMap.toMap;
    }

    def execute(sc: SparkContext, data: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val filteredData = DataFilter.filter(data, Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET", "OE_INTERACT", "OE_INTERRUPT"))));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        println("### Running the model LearnerSessionSummary ###");
        val gameList = data.map { x => x.gdata.id }.distinct().collect();
        println("### Fetching the Item data from LP ###");
        val itemData = getItemData(gameList, config.getOrElse("apiVersion", "v1").asInstanceOf[String]);
        println("### Broadcasting data to all worker nodes ###");
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);

        val itemMapping = sc.broadcast(itemData);
        val configMapping = sc.broadcast(config);
        val gameSessions = getGameSessions(filteredData);

        val screenerSummary = gameSessions.mapValues { events =>

            val firstEvent = events(0);
            val lastEvent = events.last;
            val gameId = CommonUtil.getGameId(firstEvent);
            val gameVersion = CommonUtil.getGameVersion(lastEvent);
            val assessEvents = events.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => CommonUtil.getEventTS(x) };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj.metadata;
                ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(x.edata.eks.length), metadata.get("ex_time_spent"), x.edata.eks.res, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, itemObj.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), metadata.get("max_score"), metadata.get("domain"));
            }
            val qids = assessEvents.map { x => x.edata.eks.qid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = if (oeStarts.length > 0) { Option(CommonUtil.getEventTS(oeStarts(0))) } else { Option(CommonUtil.getEventTS(firstEvent)) };
            val endTimestamp = if (oeEnds.length > 0) { Option(CommonUtil.getEventTS(oeEnds(0))) } else { Option(CommonUtil.getEventTS(lastEvent)) };

            val timeDiff = CommonUtil.getTimeDiff(firstEvent, lastEvent);
            //val timeSpent = if (oeEnds.length > 0) { CommonUtil.getTimeSpent(oeEnds.last.edata.eks.length) } else { CommonUtil.getTimeDiff(firstEvent, lastEvent) };
            val largeTimeBuff = Buffer[Double]();
            val shortTimeBuff = Buffer[Double]();
            var index = 0;
            for (index <- 0 to (events.length - 2)) {
                val first = events(index)
                val second = events(index + 1)
                val time = CommonUtil.getTimeDiff(first, second).get;
                if ((time / 60) < 10) shortTimeBuff += time; else largeTimeBuff += time;
            }
            val timeSpent = shortTimeBuff.sum;
            val interruptTime = largeTimeBuff.sum;

            val levelTransitions = events.filter { x => "OE_LEVEL_SET".equals(x.eid) }.length - 1;
            var levelMap = HashMap[String, Buffer[String]]();
            var domainMap = HashMap[String, String]();
            var tempArr = ListBuffer[String]();
            var tmpLastEvent: Event = null;
            events.foreach { x =>
                x.eid match {
                    case "OE_ASSESS" =>
                        tempArr += x.edata.eks.qid;
                        tmpLastEvent = x;
                    case "OE_LEVEL_SET" =>
                        if (levelMap.getOrElse(x.edata.eks.current, null) != null) {
                            levelMap(x.edata.eks.current) = levelMap(x.edata.eks.current) ++ tempArr;
                        } else {
                            levelMap(x.edata.eks.current) = tempArr;
                        }
                        tempArr = ListBuffer[String]();
                        domainMap(catMapping.value.getOrElse(x.edata.eks.category, getItemDomain(itemMapping.value, tmpLastEvent))) = x.edata.eks.current;
                    case _ => ;
                }
            }
            val levels = levelMap.map(f => {
                val itemCounts = f._2.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
                Map("level" -> f._1, "domain" -> "", "items" -> None, "choices" -> f._2, "noOfAttempts" -> (if (itemCounts.isEmpty) 1 else itemCounts.max));
            }).toArray;
            val loc = deviceMapping.value.getOrElse(firstEvent.did, "");
            val noOfInteractEvents = DataFilter.filter(events, Filter("edata.eks.type", "IN", Option(List("TOUCH", "DRAG", "DROP", "PINCH", "ZOOM", "SHAKE", "ROTATE", "SPEAK", "LISTEN", "WRITE", "DRAW", "START", "END", "CHOOSE", "ACTIVATE")))).length;
            val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || timeSpent == 0) 0d else BigDecimal(noOfInteractEvents / (timeSpent / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            var interactionEvents = ListBuffer[(String, Int, Double)]();
            tmpLastEvent = null;
            events.foreach { x =>
                x.eid match {
                    case "OE_INTERACT" =>
                        if (tmpLastEvent == null) tmpLastEvent = x;
                        interactionEvents += Tuple3(x.edata.eks.`type`, 1, CommonUtil.getTimeDiff(tmpLastEvent, x).get);
                        tmpLastEvent = x;
                    case _ =>
                        tmpLastEvent = x;
                }
            }
            val activitySummary = interactionEvents.groupBy(_._1).map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) } }.map(f => (f._1, ActivitySummary(f._2, f._3))).toMap;
            val eventSummary = events.groupBy { x => x.eid }.map(f => (f._1, f._2.length)).toMap;

            new SessionSummary(gameId, gameVersion, Option(levels), noOfAttempts,
                timeSpent, interruptTime, timeDiff.get, startTimestamp, endTimestamp, Option(domainMap.toMap),
                Option(levelTransitions), None, None, Option(loc),
                Option(itemResponses), DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l)), interactEventsPerMin, Option(activitySummary),
                None, Option(computeScreenSummary(events)), noOfInteractEvents, eventSummary, CommonUtil.getEventSyncTS(lastEvent));

        }
        screenerSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, SessionSummary), config: Map[String, AnyRef]): MeasuredEvent = {
        val game = userMap._2;
        val measures = Map(
            "itemResponses" -> game.itemResponses,
            "start_time" -> game.start_time,
            "end_time" -> game.end_time,
            "syncDate" -> game.syncDate,
            "timeDiff" -> game.timeDiff,
            "timeSpent" -> game.timeSpent,
            "interruptTime" -> game.interruptTime,
            "comments" -> game.comments,
            "fluency" -> game.fluency,
            "levels" -> game.levels,
            "noOfAttempts" -> game.noOfAttempts,
            "currentLevel" -> game.currentLevel,
            "noOfInteractEvents" -> game.noOfInteractEvents,
            "interactEventsPerMin" -> game.interactEventsPerMin,
            "activitySummary" -> game.activitySummary,
            "completionStatus" -> game.completionStatus,
            "screenSummary" -> game.screenSummary,
            "eventsSummary" -> game.eventsSummary,
            "noOfLevelTransitions" -> game.noOfLevelTransitions,
            "telemetryVersion" -> "1.0");
        MeasuredEvent(config.getOrElse("eventId", "ME_SESSION_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", game.dtRange),
            Dimensions(None, Option(new GData(game.id, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    }

}