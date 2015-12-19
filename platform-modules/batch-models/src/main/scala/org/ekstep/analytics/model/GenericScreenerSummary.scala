package org.ekstep.analytics.model

import org.ekstep.ilimi.analytics.framework.IBatchModel
import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.Event
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import scala.collection.mutable.Buffer
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.User
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.framework.adapter.UserAdapter
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import java.util.Date
import org.ekstep.ilimi.analytics.framework.MeasuredEvent
import org.ekstep.ilimi.analytics.framework.Context
import org.ekstep.ilimi.analytics.framework.Dimensions
import org.ekstep.ilimi.analytics.framework.PData
import org.ekstep.ilimi.analytics.framework.MEEdata
import org.ekstep.ilimi.analytics.framework.GData
import org.ekstep.ilimi.analytics.framework.adapter.ItemAdapter
import org.ekstep.ilimi.analytics.framework.Questionnaire
import scala.collection.mutable.HashMap
import org.ekstep.ilimi.analytics.framework.Item
import scala.collection.mutable.ListBuffer
import org.apache.spark.broadcast.Broadcast
import org.ekstep.ilimi.analytics.framework.UserProfile
import org.ekstep.ilimi.analytics.framework.JobContext

/**
 * @author Santhosh
 */

/**
 * Case class to hold the item responses 
 */
case class ItemResponse(itemId: Option[String], itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Option[Array[String]], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Option[Int], timeStamp: Option[Long], maxScore: Option[AnyRef], domain: Option[String]);

/**
 * Case class to hold the screener summary
 */
case class ScreenerSummary(id: Option[String], ver: Option[String], levels: Option[Array[Map[String, Any]]], noOfAttempts: Int, timeSpent: Option[Double], startTimestamp: Option[Long], endTimestamp: Option[Long], currentLevel: Option[Map[String, String]], noOfLevelTransitions: Option[Int], comments: Option[String], fluency: Option[Int], loc: Option[String]);

/**
 * Generic Screener Summary Model
 */
class GenericScreenerSummary extends IBatchModel with Serializable {
    

    /**
     * Get level to items mapping from Questionnaires
     */
    private def getLevelItems(questionnaires: Array[Questionnaire]) : Map[String, Array[String]] = {
        var levelMap = HashMap[String, Array[String]]();
        if(questionnaires.length > 0) {
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
        if(questionnaires.length > 0) {
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
    private def getItem(itemMapping: Broadcast[Map[String, (Item, String)]], event: Event) : (Item, String) = {
        itemMapping.value.getOrElse(event.edata.eks.qid.getOrElse(""), (Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]())), "numeracy"));
    }

    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
    
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val questionnaires = ItemAdapter.getQuestionnaires(config.getOrElse("contentId", "").asInstanceOf[String]);
        val catMapping = sc.broadcast(Map[String, String]("READING" -> "literacy", "MATH" -> "numeracy"));
        val deviceMapping = sc.broadcast(JobContext.deviceMapping);
        val itemMapping = sc.broadcast(getItemMapping(questionnaires));
        val levelMapping = sc.broadcast(getLevelItems(questionnaires));
        val configMapping = sc.broadcast(config);
        val userProfileMapping = sc.broadcast(UserAdapter.getUserProfileMapping());
        val itemResponses = events.filter { x => x.uid.nonEmpty && CommonUtil.getEventId(x).equals("OE_ASSESS") }
            .map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                x.map { x => 
                    val itemObj = getItem(itemMapping, x);
                    val metadata = itemObj._1.metadata;
                    ItemResponse(x.edata.eks.qid, metadata.get("type"), metadata.get("qlevel"), CommonUtil.getTimeSpent(x.edata.eks.length), metadata.get("ex_time_spent"), x.edata.eks.res, metadata.get("ex_res"), metadata.get("inc_res"), itemObj._1.mc, itemObj._1.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), metadata.get("max_score"), Option(itemObj._2)); 
                }
            };
        val screenerSummary = events.filter { x => x.uid.nonEmpty }
            .map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                val distinctEvents = x.distinct;
                val assessEvents = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_ASSESS") }.sortBy { x => CommonUtil.getEventTS(x) };
                val qids = assessEvents.map { x => x.edata.eks.qid.get}.filter { x => x != null };
                val qidMap = qids.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
                val noOfAttempts = if(qidMap.isEmpty) 1 else qidMap.max;
                val oeStarts = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_START") };
                val oeEnds = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_END") };
                val startTimestamp = if (oeStarts.length > 0) { Option(CommonUtil.getEventTS(oeStarts(0))) } else { Option(0l) };
                val endTimestamp = if (oeEnds.length > 0) { Option(CommonUtil.getEventTS(oeEnds(0))) } else { Option(0l) };
                val timeSpent = if (oeEnds.length > 0) { CommonUtil.getTimeSpent(oeEnds.last.edata.eks.length) } else { Option(0d) };
                val levelTransitions = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_LEVEL_SET") }.length - 1;
                var levelMap = HashMap[String, Buffer[String]]();
                var domainMap = HashMap[String, String]();
                var tempArr = ListBuffer[String]();
                var lastEvent: Event = null;
                distinctEvents.foreach { x =>
                    CommonUtil.getEventId(x) match {
                        case "OE_ASSESS" => 
                            tempArr += x.edata.eks.qid.get;
                            lastEvent = x;
                        case "OE_LEVEL_SET" =>
                            if(levelMap.getOrElse(x.edata.eks.current.get, null) != null) {
                                levelMap(x.edata.eks.current.get) = levelMap(x.edata.eks.current.get) ++ tempArr;
                            } else {
                                levelMap(x.edata.eks.current.get) = tempArr;    
                            }
                            tempArr = ListBuffer[String]();
                            domainMap(catMapping.value.getOrElse(x.edata.eks.category.getOrElse(""), getItem(itemMapping, lastEvent)._2)) = x.edata.eks.current.get;
                        case _ => ;
                            
                    }
                }
                val levels = levelMap.map(f => {
                    val itemCounts = f._2.groupBy { x => x }.map(f => (f._1, f._2.length)).map(f => f._2);
                    Map("level" -> f._1, "domain" -> "", "items" -> levelMapping.value.get(f._1), "choices" -> f._2, "noOfAttempts" -> (if (itemCounts.isEmpty) 1 else itemCounts.max));
                }).toArray;
                val loc = deviceMapping.value.getOrElse(distinctEvents.last.did.get, "");
                ScreenerSummary(Option(CommonUtil.getGameId(x(0))), Option(CommonUtil.getGameVersion(x(0))), Option(levels), noOfAttempts, timeSpent, startTimestamp, endTimestamp, Option(domainMap.toMap), Option(levelTransitions), None, None, Option(loc));
            }
        itemResponses.join(screenerSummary, 1).map(f => {
            getMeasuredEvent(f, userProfileMapping.value, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    /**
     * Get the measured event from the UserMap
     */
    private def getMeasuredEvent(userMap: (String, (Buffer[ItemResponse], ScreenerSummary)), userMapping: Map[String, UserProfile], config: Map[String, AnyRef]): MeasuredEvent = {
        val game = userMap._2._2;
        val user = userMapping.getOrElse(userMap._1, UserProfile(userMap._1, "NA", 0));
        val measures = Map(
            "itemResponses" -> userMap._2._1,
            "startTime" -> game.startTimestamp,
            "endTime" -> game.endTimestamp,
            "timeSpent" -> game.timeSpent,
            "comments" -> game.comments,
            "fluency" -> game.fluency,
            "levels" -> game.levels,
            "noOfAttempts" -> game.noOfAttempts,
            "currentLevel" -> game.currentLevel,
            "noOfLevelTransitions" -> game.noOfLevelTransitions
        );
        MeasuredEvent("ME_SCREENER_SUMMARY", System.currentTimeMillis(), "1.0", Option(userMap._1), None, None, 
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenericScreenerSummary").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, None, None), 
                Dimensions(None, Option(GData(game.id, game.ver)), None, None, Option(user), game.loc), 
                MEEdata(measures));
    }

}