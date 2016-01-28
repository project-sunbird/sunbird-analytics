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
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.adapter.ContentAdapter
import org.ekstep.analytics.framework.adapter.ItemAdapter
import org.ekstep.analytics.framework.DtRange
import org.joda.time.DateTime

case class Evidence(learner_id: String, itemId: String, itemMC: String, score: Int, maxScore: Int)
case class LearnerProficiency(learner_id: String, proficiency: Map[String, Double], start_time: DateTime, end_time: DateTime, model_params: Map[String, String])
case class ModelParam(concept: String, alpha: Double, beta: Double)
case class LearnerId(learner_id: String)

object ProficiencyUpdater extends IBatchModel[MeasuredEvent] with Serializable {
    
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

        // Get learner previous state
        //val prevLearnerState = events.map { x => LearnerId(x.uid.get) }.joinWithCassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map(f => (f._1.learner_id, f._2))

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
                            var conceptsMaxScoreMap = Map[String, AnyRef]();
                            if (itemMC.isEmpty && itemMC.length == 0) {
                                if (gameIdBroadcast.value.contains(gameId)) {
                                    contentId = gameId;
                                    graphId = idSubMapBroadcast.value.get(contentId).get;
                                } else if (codeIdMapBroadcast.value.contains(gameId)) {
                                    contentId = codeIdMapBroadcast.value.get(gameId).get;
                                    graphId = idSubMapBroadcast.value.get(contentId).get;
                                }
                                conceptsMaxScoreMap = ItemAdapter.getItemConceptMaxScore(contentId, itemId, configMapping.value.getOrElse("apiVersion", "v1").asInstanceOf[String]);
                                val item = conceptsMaxScoreMap.get("concepts").get.asInstanceOf[Array[String]];
                                if (item != null) itemMC = item.toList;
                                else itemMC = null;

                            }
                            val itemMMC = f.getOrElse("mmc", List()).asInstanceOf[List[String]]
                            val score = f.get("score").get.asInstanceOf[Int]
                            var maxScore = f.getOrElse("maxScore", 0).asInstanceOf[Int]

                            if (maxScore == 0) maxScore = conceptsMaxScoreMap.get("maxScore").get.asInstanceOf[Option[Int]].get

                            val timeSpent = f.get("timeSpent").get.asInstanceOf[Double]
                            val itemMisconception = Array[String]();
                            //Assessment(learner_id, itemId, itemMC, itemMMC, normScore, maxScore, itemMisconception, timeSpent)
                            (learner_id, itemId, itemMC, score, maxScore, eventStartTimestamp, eventEndTimestamp)
                        }
                }.flatten.filter(p => ((p._3) != null)).toList
                    .map { x =>
                        val mc = x._3;
                        val itemMc = mc.map { f => ((x._1), (x._2), (f), (x._4), (x._5), (x._6), (x._7)); }
                        itemMc;
                    }.flatten
                itemResponses;
            }.map(x => x._2).flatMap(f => f).map(f => (f._1, Evidence(f._1, f._2, f._3, f._4, f._5), f._6, f._7)).groupBy(f => f._1);

        //val joinedRDD = itemData.joinWithCassandraTable("learner_db", "proficiencyparams", SomeColumns("learner_id","concept","alpha","beta"), SomeColumns("learner_id"))
        val prevLearnerState = newEvidences.map { x => LearnerId(x._1) }.joinWithCassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map(f => (f._1.learner_id, f._2))

        val joinedRDD = newEvidences.leftOuterJoin(prevLearnerState);
        val lp = joinedRDD.mapValues(f => {
            val evidences = f._1
            val prevLearnerState = f._2.getOrElse(null);
            var conceptModelParamsPrv = Buffer[ModelParam]();
            var prvProficiencyMap = Map[String, Double]();

            if (null != prevLearnerState && null != prevLearnerState.model_params) {
                prvProficiencyMap = prevLearnerState.proficiency;
                conceptModelParamsPrv = prevLearnerState.model_params.map(f => {
                    val params = JSONUtils.deserialize[Map[String, Double]](f._2);
                    ModelParam(f._1, params.getOrElse("alpha", 0.5d), params.getOrElse("beta", 1d));
                }).toBuffer;
            }
            val conceptModelParamMap = conceptModelParamsPrv.map { x => (x.concept, (x.alpha, x.beta)) }.toMap

            val startTime = evidences.last._3
            val endTime = evidences.last._4
            var proficiencyMap = Map[String, Double]();
            var modelParams = Map[String, String]();
            evidences.foreach { x =>
                val evidence = x._2
                var params = Map[String, Double]();
                val concept = evidence.itemMC
                val score = evidence.score;
                val maxScore = evidence.maxScore
                val N = maxScore
                var alpha = 0.5d;
                var beta = 1d
                if (conceptModelParamMap.contains(concept)) {
                    alpha = conceptModelParamMap.get(concept).get._1
                    beta = conceptModelParamMap.get(concept).get._2
                }
                val alphaNew = alpha + score;
                val betaNew = beta + N - score;
                val pi = (alphaNew / (alphaNew + betaNew));
                params += ("alpha" -> alphaNew)
                params += ("beta" -> betaNew)
                proficiencyMap += (concept -> pi)
                modelParams += (concept -> JSONUtils.serialize(params))
            }
            var prvModelParams = Map[String, String]();
            if (prevLearnerState != null) {
                prevLearnerState.model_params.foreach { x =>
                    val map = JSONUtils.deserialize[Map[String, Double]](x._2);
                    prvModelParams += (x._1 -> JSONUtils.serialize(Map("alpha" -> map.get("alpha").get, "beta" -> map.get("beta").get)))
                }
            }
            (prvProficiencyMap ++ proficiencyMap, new DateTime(startTime), new DateTime(endTime), prvModelParams ++ modelParams);
        }).map(f => {
            LearnerProficiency(f._1, f._2._1, f._2._2, f._2._3, f._2._4);
        });
        lp.saveToCassandra("learner_db", "learnerproficiency");
        lp.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEvent(userProf: LearnerProficiency, config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = Map(
            "proficiency" -> userProf.proficiency,
            "start_ts" -> userProf.start_time.getMillis,
            "end_ts" -> userProf.end_time.getMillis);
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_PROFICIENCY_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userProf.learner_id), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ProficiencyUpdater").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", DtRange(userProf.start_time.getMillis, userProf.end_time.getMillis)),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}