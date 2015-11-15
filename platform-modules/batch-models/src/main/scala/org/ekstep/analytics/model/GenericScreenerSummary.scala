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

/**
 * @author Santhosh
 */
class GenericScreenerSummary extends IBatchModel {

    def getLength(len: Option[AnyRef]): Option[Double] = {
        if (len.nonEmpty) {
            if (len.get.isInstanceOf[String]) {
                Option(len.get.asInstanceOf[String].toDouble)
            } else if (len.get.isInstanceOf[Double]) {
                Option(len.get.asInstanceOf[Double])
            } else if (len.get.isInstanceOf[Int]) {
                Option(len.get.asInstanceOf[Int].toDouble)
            } else {
                Option(0d);
            }
        } else {
            Option(0d);
        }
    }
    
    def getItemMapping(questionnaires: Array[Questionnaire]): Map[String, Item] = {
        var itemMap = HashMap[String, Item]();
        if(questionnaires.length > 0) {
            questionnaires.foreach { x => 
                x.items.foreach { x =>  
                    itemMap(x.id) = x;    
                }
            }
        }
        itemMap.toMap;
    }

    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val questionnaires = ItemAdapter.getQuestionnaires(jobParams.getOrElse("contentId", "").asInstanceOf[String]);
        val itemMapping = sc.broadcast(getItemMapping(questionnaires));
        val userMapping = sc.broadcast(UserAdapter.getUserMapping());
        val langMapping = sc.broadcast(UserAdapter.getLanguageMapping());
        val userQuestions = events.filter { x => x.uid.nonEmpty && CommonUtil.getEventId(x).equals("OE_ASSESS") }
            .map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(10))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                x.map { x => 
                    val item = itemMapping.value.getOrElse(x.edata.eks.qid.get, Item("", Map(), Option(Array[String]()), Option(Array[String]()), Option(Array[String]())));
                    Question(x.edata.eks.qid, item.metadata.get("type"), item.metadata.get("qlevel"), getLength(x.edata.eks.length), item.metadata.get("exTimeSpent"), x.edata.eks.res, item.metadata.get("exRes"), item.metadata.get("incRes"), item.mc, item.mmc, x.edata.eks.score, Option(CommonUtil.getEventTS(x)), item.metadata.get("maxScore")) 
                }
            };
        val userGame = events.filter { x => x.uid.nonEmpty }
            .map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(10))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                val distinctEvents = x.distinct;
                val assessEvents = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_ASSESS") }.sortBy { x => CommonUtil.getEventTS(x) };
                val qids = assessEvents.map { x => x.edata.eks.qid.getOrElse("") };
                val secondChance = (qids.length - qids.distinct.length) > 0;
                val noOfSessions = Option(distinctEvents.filter { x => CommonUtil.getEventId(x).equals("GE_LAUNCH_GAME") }.length);
                val oeStarts = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("GE_LAUNCH_GAME") };
                val oeEnds = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_END") };
                val startTimestamp = if (oeStarts.length > 0) { Option(CommonUtil.getEventTS(oeStarts(0))) } else { Option(0l) };
                val endTimestamp = if (oeEnds.length > 0) { Option(CommonUtil.getEventTS(oeEnds(0))) } else { Option(0l) };
                val timeSpent = if (oeEnds.length > 0) { getLength(oeEnds.last.edata.eks.length) } else { Option(0d) };
                var levelMap = distinctEvents.filter { x => CommonUtil.getEventId(x).equals("OE_LEVEL_SET") }.map { x => (x.edata.eks.category.getOrElse(""), x.edata.eks.current.getOrElse("")) }.toMap;
                Game(Option(CommonUtil.getGameId(x(0))), Option(CommonUtil.getGameVersion(x(0))), Option(levelMap.toMap), secondChance, noOfSessions, timeSpent, startTimestamp, endTimestamp)
            }
        userQuestions.join(userGame, 1).map(f => {
            getComputedEvent(f, userMapping.value, langMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    def getComputedEvent(userMap: (String, (Buffer[Question], Game)), userMapping: Map[String, User], langMapping: Map[Int, String]): MeasuredEvent = {
        val game = userMap._2._2;
        val user = userMapping.getOrElse(userMap._1, User("Anonymous", "Anonymous", "Anonymous", "Unknown", new Date(), 0));
        val measures = Map(
            "itemResponses" -> userMap._2._1,
            "startTime" -> game.startTimestamp,
            "endTime" -> game.endTimestamp,
            "timeSpent" -> game.timeSpent,
            "comments" -> "",
            "fluency" -> 0,
            "levels" -> Array(),
            "givenSecondChance" -> game.secondChance,
            "currentLevel" -> game.level,
            "noOfLevelTransitions" -> 0
        );
        MeasuredEvent("ME_SCREENER_SUMMARY", System.currentTimeMillis(), "1.0", Option(userMap._1), None, None, 
                Context(PData("AnalyticsDataPipeline", "GenericScreenerSummary", "1.0"), None, None, None), 
                Dimensions(None, Option(GData(game.id, game.ver)), None, None, Option(user)), 
                MEEdata(measures));
    }

}

case class Question(itemId: Option[String], itype: Option[AnyRef], ilevel: Option[AnyRef], timeSpent: Option[Double], exTimeSpent: Option[AnyRef], res: Option[Array[String]], exRes: Option[AnyRef], incRes: Option[AnyRef], mc: Option[AnyRef], mmc: Option[AnyRef], score: Option[Int], timeStamp: Option[Long], maxScore: Option[AnyRef]);
case class Game(id: Option[String], ver: Option[String], level: Option[Map[String, String]], secondChance: Boolean, noOfSessions: Option[Int], timeSpent: Option[Double], startTimestamp: Option[Long], endTimestamp: Option[Long]);
