package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Event
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.User
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.adapter.UserAdapter
import org.ekstep.analytics.framework.MeasuredEvent
import java.util.Date
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.GData
import org.ekstep.analytics.framework.adapter.ItemAdapter
import org.ekstep.analytics.framework.Questionnaire
import scala.collection.mutable.HashMap
import org.ekstep.analytics.framework.Item
import scala.collection.mutable.ListBuffer
import org.apache.spark.broadcast.Broadcast
import org.ekstep.analytics.framework.UserProfile
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.SessionBatchModel

/**
 * @author Santhosh
 */

/**
 * Case class to hold the item responses
 */
case class ItemResponse(itemId: String, itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Array[String], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Int, time_stamp: Option[Long], maxScore: Option[AnyRef], domain: Option[String]);

case class ActivitySummary(count: Int, timeSpent: Double)

/**
 * Case class to hold the screener summary
 */
case class SessionSummary(id: String, ver: String, levels: Option[Array[Map[String, Any]]], noOfAttempts: Int, 
        timeSpent: Double, interruptTime: Double, start_ts: Option[Long], end_ts: Option[Long], currentLevel: Option[Map[String, String]], 
        noOfLevelTransitions: Option[Int], comments: Option[String], fluency: Option[Int], loc: Option[String], 
        itemResponses: Option[Buffer[ItemResponse]], dtRange: DtRange, interactEventsPerMin: Double, activitySummary: Option[Map[String,ActivitySummary]],
        completionStatus: Option[Boolean], screenSummary: Option[Map[String, Double]], noOfInteractEvents: Int, eventsSummary: Map[String, Int],
        syncDate: Long);

/**
 * Generic Screener Summary Model
 */
object GenericSessionSummary extends SessionBatchModel[Event] with Serializable {

    /**
     * Get level to items mapping from Questionnaires
     */
    private def getLevelItems(questionnaires: Array[Questionnaire]): Map[String, Array[String]] = {
        var levelMap = HashMap[String, Array[String]]();
        if (questionnaires.length > 0) {
            questionnaires.foreach { x =>
                x.itemSets.foreach { y =>
                    levelMap(y.metadata.getOrElse("level", "").asInstanceOf[String]) = y.items.map { z => z.id }
                }
            }
        }
        levelMap.toMap;
    }

    /**
     * Get Item id to Item mapping from Array of Questionnaires
     */
    private def getItemMapping(questionnaires: Array[Questionnaire]): Map[String, (Item, String)] = {
        var itemMap = HashMap[String, (Item, String)]();
        if (questionnaires.length > 0) {
            questionnaires.foreach { x =>
                val domain = x.metadata.getOrElse("domain", "").asInstanceOf[String];
                x.items.foreach { y =>
                    itemMap(y.id) = (y, domain);
                }
            }
        }
        itemMap.toMap;
    }

    /**
     * Get item from broadcast item mapping variable
     */
    private def getItem(itemMapping: Broadcast[Map[String, (Item, String)]], event: Event): (Item, String) = {
        itemMapping.value.getOrElse(event.edata.eks.qid, (Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]())), "numeracy"));
    }

    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val questionnaires = ItemAdapter.getQuestionnaires(config.getOrElse("contentId", "").asInstanceOf[String]);
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);
        val itemMapping = sc.broadcast(getItemMapping(questionnaires));
        val levelMapping = sc.broadcast(getLevelItems(questionnaires));
        val configMapping = sc.broadcast(config);
        val gameSessions = getGameSessions(events);

        val screenerSummary = gameSessions.mapValues { x =>
            val distinctEvents = x;
            val assessEvents = distinctEvents.filter { x => "OE_ASSESS".equals(x.eid) }.sortBy { x => CommonUtil.getEventTS(x) };
            val itemResponses = assessEvents.map { x =>
                val itemObj = getItem(itemMapping, x);
                val metadata = itemObj._1.metadata;
                ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(x.edata.eks.length), metadata.get("ex_time_spent"), x.edata.eks.res, metadata.get("ex_res"), metadata.get("inc_res"), itemObj._1.mc, itemObj._1.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), metadata.get("max_score"), Option(itemObj._2));
            }
            val qids = assessEvents.map { x => x.edata.eks.qid }.filter { x => x != null };
            val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
            val noOfAttempts = if (qidMap.isEmpty) 1 else qidMap.max;
            val oeStarts = distinctEvents.filter { x => "OE_START".equals(x.eid) };
            val oeEnds = distinctEvents.filter { x => "OE_END".equals(x.eid) };
            val startTimestamp = if (oeStarts.length > 0) { Option(CommonUtil.getEventTS(oeStarts(0))) } else { Option(CommonUtil.getEventTS(x(0))) };
            val endTimestamp = if (oeEnds.length > 0) { Option(CommonUtil.getEventTS(oeEnds(0))) } else { Option(CommonUtil.getEventTS(distinctEvents.last)) };
            val timeSpent = if (oeEnds.length > 0) { CommonUtil.getTimeSpent(oeEnds.last.edata.eks.length) } else { CommonUtil.getTimeDiff(distinctEvents(0), distinctEvents.last) };
            val levelTransitions = distinctEvents.filter { x => "OE_LEVEL_SET".equals(x.eid) }.length - 1;
            var levelMap = HashMap[String, Buffer[String]]();
            var domainMap = HashMap[String, String]();
            var tempArr = ListBuffer[String]();
            var lastEvent: Event = null;
            distinctEvents.foreach { x =>
                x.eid match {
                    case "OE_ASSESS" =>
                        tempArr += x.edata.eks.qid;
                        lastEvent = x;
                    case "OE_LEVEL_SET" =>
                        if (levelMap.getOrElse(x.edata.eks.current, null) != null) {
                            levelMap(x.edata.eks.current) = levelMap(x.edata.eks.current) ++ tempArr;
                        } else {
                            levelMap(x.edata.eks.current) = tempArr;
                        }
                        tempArr = ListBuffer[String]();
                        domainMap(catMapping.value.getOrElse(x.edata.eks.category, getItem(itemMapping, lastEvent)._2)) = x.edata.eks.current;
                    case _ => ;
                }
            }
            val levels = levelMap.map(f => {
                val itemCounts = f._2.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
                Map("level" -> f._1, "domain" -> "", "items" -> levelMapping.value.get(f._1), "choices" -> f._2, "noOfAttempts" -> (if (itemCounts.isEmpty) 1 else itemCounts.max));
            }).toArray;
            val loc = deviceMapping.value.getOrElse(distinctEvents.last.did, "");
            val noOfInteractEvents = distinctEvents.filter { x => "OE_INTERACT".equals(x.eid) }.length;
            val interactEventsPerMin:Double = if(noOfInteractEvents == 0 || timeSpent.get == 0) 0d else BigDecimal(noOfInteractEvents/(timeSpent.get/60)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            var interactionEvents = ListBuffer[(String, Int, Double)]();
            lastEvent = null;
            distinctEvents.foreach { x => 
                x.eid match {
                    case "OE_INTERACT" =>
                        if(lastEvent == null) lastEvent = x;
                        interactionEvents += Tuple3(x.edata.eks.`type`, 1, CommonUtil.getTimeDiff(lastEvent, x).get);
                        lastEvent = x;
                    case _ => 
                        lastEvent = x;
                }    
            }
            val activitySummary = interactionEvents.groupBy(_._1).map{case (group: String, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2, a._3 + b._3)} }.map(f => (f._1, ActivitySummary(f._2, f._3))).toMap;
            val eventSummary = distinctEvents.groupBy { x => x.eid }.map(f => (f._1, f._2.length)).toMap;
            SessionSummary(CommonUtil.getGameId(x(0)), CommonUtil.getGameVersion(x(0)), Option(levels), noOfAttempts, 
                    timeSpent.get, 0d, startTimestamp, endTimestamp, Option(domainMap.toMap), 
                    Option(levelTransitions), None, None, Option(loc), 
                    Option(itemResponses), DtRange(startTimestamp.getOrElse(0l), endTimestamp.getOrElse(0l)), interactEventsPerMin, Option(activitySummary),
                    None, None, noOfInteractEvents, eventSummary, CommonUtil.getEventSyncTS(distinctEvents.last));
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
            "start_ts" -> game.start_ts,
            "end_ts" -> game.end_ts,
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