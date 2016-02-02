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
 * @author Santhosh
 */

/**
 * Generic Screener Summary Model
 */
object LearnerSessionSummaryV2 extends SessionBatchModel[TelemetryEventV2] with Serializable {

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, Item], event: TelemetryEventV2): Item = {
        val item = itemMapping.getOrElse(event.edata.eks.itemid, null);
        if (null != item) {
            return item;
        }
        return Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]()));
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItemDomain(itemMapping: Map[String, Item], event: TelemetryEventV2): String = {
        val item = itemMapping.getOrElse(event.edata.eks.itemid, null);
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
    def computeScreenSummary(firstEvent: TelemetryEventV2, lastEvent: TelemetryEventV2, navigateEvents: Buffer[TelemetryEventV2]): Map[String, Double] = {

        var stageMap = HashMap[String, Double]();
        if (navigateEvents.length > 0) {
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

        }

        stageMap.toMap;
    }

    def execute(sc: SparkContext, data: RDD[TelemetryEventV2], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        println("### Running the model LearnerSessionSummaryV2 ###");
        val gameList = data.map { x => x.gdata.id }.distinct().collect();
        println("### Fetching the Item data from LP ###");
        val itemData = getItemData(gameList, config.getOrElse("apiVersion", "v1").asInstanceOf[String]);
        println("### Broadcasting data to all worker nodes ###");
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);

        val itemMapping = sc.broadcast(itemData);
        val configMapping = sc.broadcast(config);
        val gameSessions = getGameSessionsV2(data);

        val screenerSummary = gameSessions.mapValues { events =>

            val firstEvent = events(0);
            val lastEvent = events.last;
            val gameId = firstEvent.gdata.id;
            val gameVersion = firstEvent.gdata.ver;
            val assessEvents = events.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => x.ets };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj.metadata;
                val res = if(x.edata.eks.res != null ) x.edata.eks.res else Array[AnyRef]();
                ItemResponse(x.edata.eks.itemid, metadata.get("type"), metadata.get("qlevel"), Option(x.edata.eks.duration), metadata.get("ex_time_spent"), res.map { x => x.asInstanceOf[String] }, metadata.get("ex_res"), metadata.get("inc_res"), itemObj.mc, itemObj.mmc, x.edata.eks.score, Option(x.ets), metadata.get("max_score"), metadata.get("domain"));
            }
            val qids = assessEvents.map { x => x.edata.eks.itemid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = firstEvent.ets;
            val endTimestamp = lastEvent.ets;
            val timeSpent = if (oeEnds.length > 0) { Option(oeEnds.last.edata.eks.duration.toDouble) } else { CommonUtil.getTimeDiff(firstEvent.ets, lastEvent.ets) };
            val levelTransitions = events.filter { x => "OE_LEVEL_SET".equals(x.eid) }.length - 1;
            var levelMap = HashMap[String, Buffer[String]]();
            var domainMap = HashMap[String, String]();
            var tempArr = ListBuffer[String]();
            var tmpLastEvent: TelemetryEventV2 = null;
            events.foreach { x =>
                x.eid match {
                    case "OE_ASSESS" =>
                        tempArr += x.edata.eks.itemid;
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
            val noOfInteractEvents = events.filter { x => "OE_INTERACT".equals(x.eid) }.length;
            val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || timeSpent.get == 0) 0d else CommonUtil.roundDouble((noOfInteractEvents / (timeSpent.get / 60)), 2);
            var interactionEvents = ListBuffer[(String, Int, Double)]();
            tmpLastEvent = null;
            events.foreach { x =>
                x.eid match {
                    case "OE_INTERACT" =>
                        if (tmpLastEvent == null) tmpLastEvent = x;
                        interactionEvents += Tuple3(x.edata.eks.`type`, 1, CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get);
                        tmpLastEvent = x;
                    case _ =>
                        tmpLastEvent = x;
                }
            }
            val activitySummary = interactionEvents.groupBy(_._1).map { case (group: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2, a._3 + b._3) } }.map(f => (f._1, ActivitySummary(f._2, CommonUtil.roundDouble(f._3, 2)))).toMap;
            val eventSummary = events.groupBy { x => x.eid }.map(f => (f._1, f._2.length)).toMap;
            val navigateEvents = DataFilter.filter(events, Filter("eid", "EQ", Option("OE_NAVIGATE")));
            SessionSummary(gameId, gameVersion, Option(levels), noOfAttempts, timeSpent.get, 0d, Option(startTimestamp),
                Option(endTimestamp), Option(domainMap.toMap), Option(levelTransitions), None, None, Option(loc),
                Option(itemResponses), DtRange(startTimestamp, endTimestamp), interactEventsPerMin,
                Option(activitySummary), None, Option(computeScreenSummary(firstEvent, lastEvent, navigateEvents)), noOfInteractEvents, eventSummary,
                lastEvent.ets);
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
            "start_time" -> game.start_ts,
            "end_time" -> game.end_ts,
            "syncDate" -> game.syncDate,
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
            "telemetryVersion" -> "2.0");
        MeasuredEvent(config.getOrElse("eventId", "ME_SESSION_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "LearnerSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "SESSION", game.dtRange),
            Dimensions(None, Option(new GData(game.id, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    }

}