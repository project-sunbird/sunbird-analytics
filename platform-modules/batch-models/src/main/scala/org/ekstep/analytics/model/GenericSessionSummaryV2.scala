package org.ekstep.analytics.model

import scala.BigDecimal
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.Item
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Questionnaire
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.adapter.ContentAdapter
import org.ekstep.analytics.framework.adapter.ItemAdapter
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */

/**
 * Generic Screener Summary V2 Model
 */
object GenericSessionSummaryV2 extends SessionBatchModel[Event] with Serializable {

    /**
     * Get level to items mapping from Questionnaires
     */
    private def getLevelItems(contentMap: Map[String, Array[Questionnaire]]): Map[String, Map[String, Array[String]]] = {
        if (contentMap.size > 0) {
            contentMap.filter(_._2 != null).mapValues(questionnaires => {
                questionnaires.map { x =>
                    x.itemSets.map { y =>
                        (y.metadata.getOrElse("level", "").asInstanceOf[String], y.items.map { z => z.id })
                    }
                }.reduce((a, b) => a ++ b).toMap;
            })
        } else {
            Map[String, Map[String, Array[String]]]();
        }
    }

    /**
     * Get Item id to Item mapping from Array of Questionnaires
     */
    private def getItemMapping(contentMap: Map[String, Array[Questionnaire]]): Map[String, (Item, String)] = {
        if (contentMap.size > 0) {
            contentMap.filter(_._2 != null).mapValues(questionnaires => {
                questionnaires.map { x =>
                    val domain = x.metadata.getOrElse("domain", "").asInstanceOf[String];
                    x.items.map { y => (y.id, y, domain) }
                }.reduce((a, b) => a ++ b);
            }).map(f => f._2).reduce((a, b) => a ++ b).map(f => (f._1, (f._2, f._3))).toMap;
        } else {
            Map[String, (Item, String)]();
        }
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Map[String, (Item, String)], event: Event): (Item, String) = {
        val item = itemMapping.getOrElse(event.edata.eks.qid, null);
        if (null != item) {
            return item;
        }
        return (Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]())), "numeracy");
    }

    private def getItemData(sc: SparkContext, games: Array[String]): Map[String, Array[Questionnaire]] = {

        val lpGameList = ContentAdapter.getGameList();
        val gameIds = lpGameList.map { x => x.identifier };
        val codeIdMap: Map[String, String] = lpGameList.map { x => (x.code, x.identifier) }.toMap;
        games.map { gameId =>
            {
                if (gameIds.contains(gameId)) {
                    (gameId, ItemAdapter.getQuestionnaires(gameId))
                } else if (codeIdMap.contains(gameId)) {
                    (gameId, ItemAdapter.getQuestionnaires(codeIdMap.get(gameId).get))
                } else {
                    null;
                }
            }
        }.filter(x => x != null).toMap;
    }

    def execute(sc: SparkContext, data: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        
        println("### Running the model GenericSessionSummaryV2 ###");
        val gameList = data.map { x => x.gdata.id }.distinct().collect();
        println("### Fetching the Item data from LP ###");
        val gameQuestionnaires = getItemData(sc, gameList);
        println("### Broadcasting data to all worker nodes ###");
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);
        val contentItemMapping = getItemMapping(gameQuestionnaires);
        //val contentLevelMapping = getLevelItems(gameQuestionnaires);

        val itemMapping = sc.broadcast(contentItemMapping);
        //val levelMapping = sc.broadcast(contentLevelMapping);
        val configMapping = sc.broadcast(jobParams.getOrElse(Map[String, AnyRef]()));
        val gameSessions = getGameSessions(data);

        val screenerSummary = gameSessions.mapValues { events =>

            val firstEvent = events(0);
            val lastEvent = events.last;
            val gameId = CommonUtil.getGameId(firstEvent);
            val gameVersion = CommonUtil.getGameVersion(lastEvent);
            val assessEvents = events.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => CommonUtil.getEventTS(x) };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping.value, x);
                val metadata = itemObj._1.metadata;
                ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(x.edata.eks.length), metadata.get("ex_time_spent"), x.edata.eks.res, metadata.get("ex_res"), metadata.get("inc_res"), itemObj._1.mc, itemObj._1.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), metadata.get("max_score"), Option(itemObj._2));
            }
            val qids = assessEvents.map { x => x.edata.eks.qid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = events.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = events.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = if (oeStarts.length > 0) { Option(CommonUtil.getEventTS(oeStarts(0))) } else { Option(CommonUtil.getEventTS(firstEvent)) };
            val endTimestamp = if (oeEnds.length > 0) { Option(CommonUtil.getEventTS(oeEnds(0))) } else { Option(CommonUtil.getEventTS(lastEvent)) };
            val timeSpent = if (oeEnds.length > 0) { CommonUtil.getTimeSpent(oeEnds.last.edata.eks.length) } else { CommonUtil.getTimeDiff(firstEvent, lastEvent) };
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
                        domainMap(catMapping.value.getOrElse(x.edata.eks.category, getItem(itemMapping.value, lastEvent)._2)) = x.edata.eks.current;
                    case _ => ;
                }
            }
            val levels = levelMap.map(f => {
                val itemCounts = f._2.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
                //val gameItems = levelMapping.value.getOrElse(gameId, null);
                //val items = if (gameItems != null) gameItems.get(f._1) else None;
                val items = None;
                Map("level" -> f._1, "domain" -> "", "items" -> items, "choices" -> f._2, "noOfAttempts" -> (if (itemCounts.isEmpty) 1 else itemCounts.max));
            }).toArray;
            val loc = deviceMapping.value.getOrElse(firstEvent.did, "");
            val noOfInteractEvents = events.filter { x => "OE_INTERACT".equals(x.eid) }.length;
            val interactEventsPerMin: Double = if (noOfInteractEvents == 0 || timeSpent.get == 0) 0d else BigDecimal(noOfInteractEvents / (timeSpent.get / 60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
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
            SessionSummary(gameId, gameVersion, Option(levels), noOfAttempts,
                timeSpent.get, 0d, startTimestamp, endTimestamp, Option(domainMap.toMap),
                Option(levelTransitions), None, None, Option(loc),
                Option(itemResponses), DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l)), interactEventsPerMin, Option(activitySummary),
                None, None, noOfInteractEvents, eventSummary);
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
            "startTime" -> game.startTimestamp,
            "endTime" -> game.endTimestamp,
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
            "noOfLevelTransitions" -> game.noOfLevelTransitions);
        MeasuredEvent(config.getOrElse("eventId", "ME_SESSION_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenericSessionSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.3").asInstanceOf[String]), None, "SESSION", game.dtRange),
            Dimensions(None, Option(new GData(game.id, game.ver)), None, None, None, game.loc),
            MEEdata(measures));
    }

}