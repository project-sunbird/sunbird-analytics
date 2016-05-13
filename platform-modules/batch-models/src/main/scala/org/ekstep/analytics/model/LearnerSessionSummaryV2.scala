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
import org.ekstep.analytics.updater.LearnerProfile
import org.ekstep.analytics.util.Constants
import java.net.URLEncoder

/**
 * @author Santhosh
 */

/**
 * Generic Screener Summary Model
 */
object LearnerSessionSummaryV2 extends SessionBatchModel[TelemetryEventV2] with Serializable {

    val className = "org.ekstep.analytics.model.LearnerSessionSummaryV2"
    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: TelemetryEventV2): Item = {
        val item = itemMapping.getOrElse(event.edata.eks.qid, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItemDomain(itemMapping: Map[String, Item], event: TelemetryEventV2): String = {
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
    def computeScreenSummary(firstEvent: TelemetryEventV2, lastEvent: TelemetryEventV2, navigateEvents: Buffer[TelemetryEventV2], interruptSummary: Map[String, Double]): Iterable[ScreenSummary] = {

        if (navigateEvents.length > 0) {
            var stageMap = HashMap[String, Double]();
            var prevEvent = firstEvent;
            navigateEvents.foreach { x =>
                val currStage = x.edata.eks.stageid;
                val timeDiff = CommonUtil.getTimeDiff(prevEvent.ets, x.ets).get;
                if (stageMap.getOrElse(currStage, null) == null) {
                    stageMap.put(currStage, timeDiff);
                } else {
                    stageMap.put(currStage, stageMap.get(currStage).get + timeDiff);
                }
                prevEvent = x;
            }

            val lastStage = prevEvent.edata.eks.stageto;
            val timeDiff = CommonUtil.getTimeDiff(prevEvent.ets, lastEvent.ets).get;
            if (stageMap.getOrElse(lastStage, null) == null) {
                stageMap.put(lastStage, timeDiff);
            } else {
                stageMap.put(lastStage, stageMap.get(lastStage).get + timeDiff);
            }
            stageMap.map(f => {
                ScreenSummary(f._1, CommonUtil.roundDouble((f._2 - interruptSummary.getOrElse(f._1, 0d)), 2));
            });
        } else {
            Iterable[ScreenSummary]();
        }

    }

    def computeInterruptSummary(interruptEvents: Buffer[TelemetryEventV2]): Map[String, Double] = {

        if (interruptEvents.length > 0) {
            interruptEvents.groupBy { event => event.edata.eks.stageid }.mapValues { events =>
                var prevTs: Long = 0;
                var ts: Double = 0;
                events.foreach { event =>
                    event.edata.eks.`type` match {
                        case "RESUME" =>
                            if (prevTs == 0) prevTs = event.ets;
                            ts += CommonUtil.getTimeDiff(prevTs, event.ets).get;
                            prevTs == 0
                        case _ =>
                            prevTs = event.ets;

                    }
                }
                ts;
            }.toMap;
        } else {
            Map[String, Double]();
        }
    }

    def execute(data: RDD[TelemetryEventV2], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.info("LearnerSessionSummaryV2 : execute method starting", className)
        JobLogger.debug("Filtering Events of OE_ASSESS,OE_START, OE_END, OE_LEVEL_SET, OE_INTERACT, OE_INTERRUPT,OE_NAVIGATE,OE_ITEM_RESPONSE", className)
        val v2Events = DataFilter.filter(data, Array(Filter("ver", "EQ", Option("2.0")),Filter("uid", "ISNOTEMPTY", None)));
        val filteredData = DataFilter.filter(v2Events, Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_START", "OE_END", "OE_LEVEL_SET", "OE_INTERACT", "OE_INTERRUPT", "OE_NAVIGATE", "OE_ITEM_RESPONSE"))));
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
        val gameSessions = getGameSessionsV2(filteredData);
        val contentTypeMap = contents.map { x => (x.id, (x.metadata.get("contentType"), x.metadata.get("mimeType"))) }
        val contentTypeMapping = sc.broadcast(contentTypeMap.toMap);
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        JobLogger.debug("Calculating Screen Summary", className)
        val screenerSummary = gameSessions.mapValues { events =>

            val firstEvent = events(0);
            val lastEvent = events.last;

            var partnerId = ""
            if (null != firstEvent.tags) {
                partnerId = firstEvent.tags.find(p => p.contains("partnerid")).getOrElse(Map()).getOrElse("partnerid", "").asInstanceOf[String]
            }

            val gameId = firstEvent.gdata.id;
            val gameVersion = firstEvent.gdata.ver;
            val content = contentTypeMapping.value.getOrElse(gameId, (Option("Game"), Option("application/vnd.android.package-archive")));
            val contentType = content._1;
            val mimeType = content._2;

            val assessEvents = events.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => x.ets };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj.metadata;
                val resValues = if (null == x.edata.eks.resvalues) Array[Map[String, AnyRef]]() else x.edata.eks.resvalues;
                ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), Option(x.edata.eks.length), metadata.get("ex_time_spent"), resValues.map(f => f.asInstanceOf[AnyRef]), metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, itemObj.mmc, x.edata.eks.score, Option(x.ets), metadata.get("max_score"), metadata.get("domain"));
            }
            val qids = assessEvents.map { x => x.edata.eks.qid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = firstEvent.ets;
            val endTimestamp = lastEvent.ets;

            val timeDiff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(startTimestamp, endTimestamp).get, 2);

            var tmpLastEvent: TelemetryEventV2 = null;
            val eventsWithTs = events.map { x =>
                if (tmpLastEvent == null) tmpLastEvent = x;
                val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                tmpLastEvent = x;
                (x, if (ts > idleTime) 0 else ts)
            }

            val timeSpent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);

            val levelTransitions = events.filter { x => "OE_LEVEL_SET".equals(x.eid) }.length - 1;
            var levelMap = HashMap[String, Buffer[String]]();
            var domainMap = HashMap[String, String]();
            var tempArr = ListBuffer[String]();
            tmpLastEvent = null;
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
                        domainMap(catMapping.value.getOrElse(x.edata.eks.category, getItemDomain(itemMapping.value, lastEvent))) = x.edata.eks.current;
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
            val activitySummary = interactionEvents.groupBy(_._1).map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) } }.map(f => ActivitySummary(f._1, f._2, CommonUtil.roundDouble(f._3, 2)));
            val eventSummary = events.groupBy { x => x.eid }.map(f => EventSummary(f._1, f._2.length));
            val navigateEvents = DataFilter.filter(events, Filter("eid", "EQ", Option("OE_NAVIGATE")));
            val interruptSummary = computeInterruptSummary(DataFilter.filter(events, Filter("eid", "EQ", Option("OE_INTERRUPT"))));
            val screenSummary = computeScreenSummary(firstEvent, lastEvent, navigateEvents, interruptSummary);
            val interruptTime = CommonUtil.roundDouble((timeDiff - timeSpent) + (if (interruptSummary.size > 0) interruptSummary.map(f => f._2).sum else 0d), 2);

            val did = firstEvent.did
            new SessionSummary(gameId, gameVersion, Option(levels), noOfAttempts, timeSpent, interruptTime, timeDiff, Option(startTimestamp),
                Option(endTimestamp), Option(domainMap.toMap), Option(levelTransitions), None, None, Option(loc), Option(itemResponses),
                DtRange(startTimestamp, endTimestamp), interactEventsPerMin, Option(activitySummary), None, Option(screenSummary), noOfInteractEvents,
                eventSummary, lastEvent.ets, contentType, mimeType, did, partnerId);
        }.filter(f => (f._2.timeSpent >= 0)) // Skipping the events, if timeSpent is -ve

        JobLogger.debug("'screenerSummary' joining with 'LearnerProfile' table to get group_user value for each learner", className)
        //Joining with LearnerProfile table to add group info
        val groupInfoSummary = screenerSummary.map(f => LearnerId(f._1)).distinct().joinWithCassandraTable[LearnerProfile](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE).map { x => (x._1.learner_id, (x._2.group_user, x._2.anonymous_user)) }
        val sessionSummary = screenerSummary.leftOuterJoin(groupInfoSummary)

        JobLogger.debug("Serializing 'ME_SESSION_SUMMARY' MeasuredEvent", className)
        JobLogger.info("LearnerSessionSummary: execute method Ending", className)
        sessionSummary.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, (SessionSummary, Option[(Boolean, Boolean)])), config: Map[String, AnyRef]): MeasuredEvent = {

        val game = userMap._2._1;
        val booleanTuple = userMap._2._2.getOrElse((false,false))
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
            "telemetryVersion" -> "2.0",
            "contentType" -> game.contentType,
            "mimeType" -> game.mimeType,
            "partnerId" -> game.partnerId,
            "groupUser" -> booleanTuple._1,
            "anonymousUser" -> booleanTuple._2);

        MeasuredEvent(config.getOrElse("eventId", "ME_SESSION_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), game.syncDate, "1.0", mid, Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", game.dtRange),
            Dimensions(None, Option(game.did), Option(new GData(game.id, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    }

    def main(args: Array[String]): Unit = {
        println(URLEncoder.encode("E2E_QA_Delete Content_Collection", "UTF-8"));
    }
}