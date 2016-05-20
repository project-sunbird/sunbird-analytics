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
import java.security.MessageDigest
import org.apache.log4j.Logger
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.LearnerProfile

/**
 * Case class to hold the item responses
 */
case class ItemResponse(itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Array[AnyRef], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Int, time_stamp: Option[Long], maxScore: Option[AnyRef], domain: Option[AnyRef]);

case class ActivitySummary(actType: String, count: Int, timeSpent: Double)
case class ScreenSummary(id: String, timeSpent: Double)
case class EventSummary(id: String, count: Int)

/**
 * Case class to hold the screener summary
 */
class SessionSummary(val id: String, val ver: String, val levels: Option[Array[Map[String, Any]]], val noOfAttempts: Int, val timeSpent: Double,
                     val interruptTime: Double, val timeDiff: Double, val start_time: Option[Long], val end_time: Option[Long], val currentLevel: Option[Map[String, String]],
                     val noOfLevelTransitions: Option[Int], val comments: Option[String], val fluency: Option[Int], val loc: Option[String],
                     val itemResponses: Option[Buffer[ItemResponse]], val dtRange: DtRange, val interactEventsPerMin: Double, val activitySummary: Option[Iterable[ActivitySummary]],
                     val completionStatus: Option[Boolean], val screenSummary: Option[Iterable[ScreenSummary]], val noOfInteractEvents: Int, val eventsSummary: Iterable[EventSummary],
                     val syncDate: Long, val contentType: Option[AnyRef], val mimeType: Option[AnyRef], val did: String, val tags: List[Map[String, AnyRef]]) extends Serializable {};

/**
 * @author Santhosh
 */

/**
 * Generic Screener Summary Model
 */
object LearnerSessionSummary extends SessionBatchModel[Event] with Serializable {

    val className = "org.ekstep.analytics.model.LearnerSessionSummary"

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

    /**
     * Compute screen summaries on the telemetry data produced by content app
     */
    def computeScreenSummary(events: Buffer[Event]): Iterable[ScreenSummary] = {

        val screenInteractEvents = DataFilter.filter(events, Filter("eid", "NIN", Option(List("OE_ASSESS", "OE_LEVEL_SET")))).filter { event =>
            event.eid match {
                case "OE_INTERACT" =>
                    event.edata.ext != null && event.edata.ext.stageId != null;
                case _ =>
                    true
            }
        }
        var stageMap = HashMap[String, Double]();
        var screenSummaryList = Buffer[HashMap[String, Double]]();
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
        stageMap.map { x => ScreenSummary(x._1, x._2) };
    }

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.info("LearnerSessionSummary : execute method starting", className)
        JobLogger.debug("Filtering Events of OE_ASSESS,OE_START, OE_END, OE_LEVEL_SET, OE_INTERACT, OE_INTERRUPT", className)
        val v1Events = DataFilter.filter(data, Array(Filter("ver", "EQ", Option("1.0")), Filter("uid", "ISNOTEMPTY", None)));
        val filteredData = DataFilter.filter(v1Events, Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET", "OE_INTERACT", "OE_INTERRUPT"))));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val gameList = data.map { x => x.gdata.id }.distinct().collect();
        JobLogger.debug("Fetching the Content and Item data from Learing Platform", className)
        val contents = ContentAdapter.getAllContent();
        val itemData = getItemData(contents, gameList, config.getOrElse("apiVersion", "v2").asInstanceOf[String]);
        JobLogger.debug("Broadcasting data to all worker nodes", className)
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);

        val itemMapping = sc.broadcast(itemData);
        val configMapping = sc.broadcast(config);
        JobLogger.debug("Doing Game Sessionization", className)
        val gameSessions = getGameSessions(filteredData);
        val contentTypeMap = contents.map { x => (x.id, (x.metadata.get("contentType"), x.metadata.get("mimeType"))) }
        val contentTypeMapping = sc.broadcast(contentTypeMap.toMap);
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        JobLogger.debug("Calculating Screen Summary", className)
        val screenerSummary = gameSessions.mapValues { events =>

            val firstEvent = events(0);
            val lastEvent = events.last;
            val gameId = CommonUtil.getGameId(firstEvent);
            val gameVersion = CommonUtil.getGameVersion(lastEvent);

            //            var partnerId = ""
            //            if (null != firstEvent.tags) {
            //                partnerId = firstEvent.tags.find(p => p.contains("partnerid")).getOrElse(Map()).getOrElse("partnerid", "").asInstanceOf[String]
            //            }

            val content = contentTypeMapping.value.getOrElse(gameId, (Option("Game"), Option("application/vnd.android.package-archive")));
            val contentType = content._1;
            val mimeType = content._2;
            val assessEvents = events.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => CommonUtil.getEventTS(x) };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj.metadata;
                ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(x.edata.eks.length), metadata.get("ex_time_spent"), x.edata.eks.res.asInstanceOf[Array[AnyRef]], metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, itemObj.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), metadata.get("max_score"), metadata.get("domain"));
            }
            val qids = assessEvents.map { x => x.edata.eks.qid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = Option(CommonUtil.getEventTS(firstEvent));
            val endTimestamp = Option(CommonUtil.getEventTS(lastEvent));
            val timeDiff = CommonUtil.getTimeDiff(firstEvent, lastEvent);

            var tmpLastEvent: Event = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent, x).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }

            val timeSpent = eventsWithTs.map(f => f._2).sum;
            val interruptTime = timeDiff.get - timeSpent;

            val levelTransitions = events.filter { x => "OE_LEVEL_SET".equals(x.eid) }.length - 1;
            var levelMap = HashMap[String, Buffer[String]]();
            var domainMap = HashMap[String, String]();
            var tempArr = ListBuffer[String]();

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
            var interactionEvents = eventsWithTs.filter(f => "OE_INTERACT".equals(f._1.eid)).map(f => {
                (f._1.edata.eks.`type`, 1, f._2)
            })

            val activitySummary = interactionEvents.groupBy(_._1).map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) } }.map(f => ActivitySummary(f._1, f._2, f._3));
            val eventSummary = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));
            val did = firstEvent.did
            val tags = firstEvent.tags
            new SessionSummary(gameId, gameVersion, Option(levels), noOfAttempts, timeSpent, interruptTime, timeDiff.get, startTimestamp, endTimestamp,
                Option(domainMap.toMap), Option(levelTransitions), None, None, Option(loc), Option(itemResponses), DtRange(startTimestamp.getOrElse(0l),
                    endTimestamp.getOrElse(0l)), interactEventsPerMin, Option(activitySummary), None, Option(computeScreenSummary(events)), noOfInteractEvents,
                eventSummary, CommonUtil.getEventSyncTS(lastEvent), contentType, mimeType, did, tags);

        }.filter(f => (f._2.timeSpent >= 0)) // Skipping the events, if timeSpent is -ve

        JobLogger.debug("'screenerSummary' joining with LearnerProfile table to get group_user value for each learner", className)
        //Joining with LearnerProfile table to add group info
        val groupInfoSummary = screenerSummary.map(f => LearnerId(f._1)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (x._1.learner_id, (x._2.group_user, x._2.anonymous_user)); }
        val sessionSummary = screenerSummary.leftOuterJoin(groupInfoSummary)

        JobLogger.debug("Serializing 'ME_SESSION_SUMMARY' MeasuredEvent", className)
        JobLogger.info("LearnerSessionSummary: execute method Ending", className)

        sessionSummary.map(x => {
            getMeasuredEvent(x, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, (SessionSummary, Option[(Boolean, Boolean)])), config: Map[String, AnyRef]): MeasuredEvent = {

        val game = userMap._2._1;
        val booleanTuple = userMap._2._2.getOrElse((false, false))
        val mid = CommonUtil.getMessageId("ME_SESSION_SUMMARY", userMap._1, "SESSION", game.dtRange, game.id);
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
            "telemetryVersion" -> "1.0",
            "contentType" -> game.contentType,
            "mimeType" -> game.mimeType,
            "groupUser" -> booleanTuple._1,
            "anonymousUser" -> booleanTuple._2);
        MeasuredEvent(Option(game.tags), "ME_SESSION_SUMMARY", System.currentTimeMillis(), game.syncDate, "1.0", mid, Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", game.dtRange),
            Dimensions(None, Option(game.did), Option(new GData(game.id, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    }

}