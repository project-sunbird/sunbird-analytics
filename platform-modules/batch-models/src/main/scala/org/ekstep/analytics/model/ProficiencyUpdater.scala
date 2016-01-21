package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.annotation.migration
import org.ekstep.analytics.updater.UpdateProficiencyModelParam
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.adapter.ContentAdapter
import org.ekstep.analytics.framework.adapter.ItemAdapter

case class Evidence(learner_id: String, itemId: String, itemMC: String, normScore: Double, maxScore: Int);

case class LearnerProficiency(proficiency: Map[String, Double], startTime: Long, endTime: Long)
case class ModelParam(learner_id: String, concept: String, alpha: Double, beta: Double)

class ProficiencyUpdater extends IBatchModel[MeasuredEvent] with Serializable {
    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        val lpGameList = ContentAdapter.getGameList();
        val gameIds = lpGameList.map { x => x.identifier };
        val codeIdMap: Map[String, String] = lpGameList.map { x => (x.code, x.identifier) }.toMap;
        val idSubMap: Map[String, String] = lpGameList.map { x => (x.identifier, x.subject) }.toMap;

        val gameIdBroadcast = sc.broadcast(gameIds);
        val codeIdMapBroadcast = sc.broadcast(codeIdMap);
        val idSubMapBroadcast = sc.broadcast(idSubMap);

        // Get learner concept model params
        val profParams = events.map { x => x.uid.get }.joinWithCassandraTable[ModelParam]("learner_db", "proficiencyparams").groupBy(f => f._1);
        
        // Get learner previous state
        val prevLearnerState = events.map { x => x.uid.get }.joinWithCassandraTable[LearnerProficiency]("learner_db", "learnerproficiency");
        
        val newEvidences = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                val sortedEvents = x.sortBy { x => x.ets };
                val eventStartTimestamp = sortedEvents(0).ets;
                val eventEndTimestamp = sortedEvents.last.ets;

                val itemResponses = sortedEvents.map { x =>
                    val gameId = x.dimensions.gdata.get.id
                    val learner_id = x.uid.get
                    x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemResponses").get.asInstanceOf[List[Map[String, AnyRef]]]
                        .map { f =>
                            val itemId = f.get("itemId").get.asInstanceOf[String];
                            var itemMC = f.getOrElse("mc", List()).asInstanceOf[List[String]]

                            var contentId = "";
                            var graphId = "";

                            if (itemMC.isEmpty && itemMC.length == 0) {
                                if (gameIdBroadcast.value.contains(gameId)) {
                                    contentId = gameId;
                                    graphId = idSubMapBroadcast.value.get(contentId).get;
                                } else if (codeIdMapBroadcast.value.contains(gameId)) {
                                    contentId = codeIdMapBroadcast.value.get(gameId).get;
                                    graphId = idSubMapBroadcast.value.get(contentId).get;
                                }
                                itemMC = ItemAdapter.getItemConcept(graphId, contentId, itemId).toList;
                            }
                            val itemMMC = f.getOrElse("mmc", List()).asInstanceOf[List[String]]
                            val score = f.get("score").get.asInstanceOf[Int]
                            var maxScore = f.getOrElse("maxScore", 0).asInstanceOf[Int]
                            
                            if (maxScore == 0) maxScore = ItemAdapter.getItemMaxScore(graphId, contentId, itemId);
                            
                            val timeSpent = f.get("timeSpent").get.asInstanceOf[Double]
                            val itemMisconception = Array[String]();
                            val normScore = (score / maxScore);
                            //Assessment(learner_id, itemId, itemMC, itemMMC, normScore, maxScore, itemMisconception, timeSpent)
                            (learner_id, itemId, itemMC, normScore, maxScore)
                        }
                }.flatten.filter(p=>(!p._3.isEmpty)).toList.
                map{x=>
                    val mc = x._3;
                    mc.map{f=>(x._1,x._2,f,x._4,x._5)}
                }.flatten
                itemResponses;
        }.map(x=>x._2).flatMap(f=>f).map(f => (f._1, Evidence(f._1, f._2, f._3, f._4, f._5))).groupBy(f => f._1);
              
        //val joinedRDD = itemData.joinWithCassandraTable("learner_db", "proficiencyparams", SomeColumns("learner_id","concept","alpha","beta"), SomeColumns("learner_id"))
         
        val joinedRDD = newEvidences.leftOuterJoin(prevLearnerState).leftOuterJoin(profParams);
        joinedRDD.mapValues(f => {
            val evidences = f._1._1;
            val prevLearnerState = f._1._2.getOrElse(null);
            val conceptModelParams = f._2.getOrElse(Array());
            
            // Write your logic here....
            null;
        })
                
       return null;
    }
    private def getMeasuredEvent(userMap: (String, (LearnerProficiency, DtRange)), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2._1;
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_PROFICIENCY_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ProficiencyUpdater").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", userMap._2._2),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}