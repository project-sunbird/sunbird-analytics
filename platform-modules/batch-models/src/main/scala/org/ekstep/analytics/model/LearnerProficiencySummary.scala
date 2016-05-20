package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Filter
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
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.ItemConcept
import org.apache.spark.broadcast.Broadcast
import org.ekstep.analytics.framework.LearnerId
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import java.security.MessageDigest
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger

case class Evidence(learner_id: String, itemId: String, itemMC: String, score: Int, maxScore: Int)
case class LearnerProficiency(learner_id: String, proficiency: Map[String, Double], start_time: DateTime, end_time: DateTime, model_params: Map[String, String])
case class ModelParam(concept: String, alpha: Double, beta: Double)
case class ProficiencySummary(conceptId: String, proficiency: Double)

object LearnerProficiencySummary extends IBatchModel[MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.LearnerProficiencySummary"

    def getItemConcept(item: Map[String, AnyRef], itemMapping: Map[String, ItemConcept]): Array[String] = {
        val itemId = item.get("itemId").get.asInstanceOf[String];
        //val itemMC = item.getOrElse("mc", List()).asInstanceOf[List[String]]
        val itemMC = item.get("mc").get.asInstanceOf[List[String]]
        if (itemMC.isEmpty && itemMC.length == 0) {
            val itemConcept = itemMapping.get(itemId);
            if (null == itemConcept.get.concepts) {
                Array[String]();
            } else {
                itemConcept.get.concepts;
            }
        } else {
            itemMC.toArray
        }
    }

    def getMaxScore(item: Map[String, AnyRef]): Int = {
        val maxScore = item.get("maxScore");
        if (maxScore.nonEmpty) {
            if (maxScore.get.isInstanceOf[Double]) {
                maxScore.get.asInstanceOf[Double].toInt;
            } else {
                maxScore.get.asInstanceOf[Int];
            }
        } else 0;
    }

    def getItemMaxScore(item: Map[String, AnyRef], itemMapping: Map[String, ItemConcept]): Int = {

        val itemId = item.get("itemId").get.asInstanceOf[String];
        val maxScore = getMaxScore(item);
        if (maxScore == 0) {
            val itemConcept = itemMapping.get(itemId);
            if (0 == itemConcept.get.maxScore) {
                1;
            } else {
                itemConcept.get.maxScore;
            }
        } else {
            maxScore;
        }
    }

    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.info("LearnerProficiencySummary : execute method starting", className)
        JobLogger.debug("Filtering ME_SESSION_SUMMARY events", className)
        val filteredData = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);
        JobLogger.debug("Getting game list from ContentAdapter", className)
        val lpGameList = ContentAdapter.getGameList();
        val gameIds = lpGameList.map { x => x.identifier };
        JobLogger.debug("Preparing Code-Id Map & Id-Subject Map", className)
        val codeIdMap: Map[String, String] = lpGameList.map { x => (x.code, x.identifier) }.toMap;
        val idSubMap: Map[String, String] = lpGameList.map { x => (x.identifier, x.subject) }.toMap;

        JobLogger.debug("Finding the items with missing mc", className)
        val itemsWithMissingConcepts = filteredData.map { event =>
            val ir = event.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemResponses").get.asInstanceOf[List[Map[String, AnyRef]]];
            ir.filter(item => {
                val itemMC = item.get("mc").get.asInstanceOf[List[String]];
                itemMC == null || itemMC.isEmpty
            }).map(f => (f.get("itemId").get.asInstanceOf[String], event.dimensions.gdata.get.id))
        }.filter(f => f.nonEmpty).cache();

        var itemConcepts = Map[String, ItemConcept]();
        if (itemsWithMissingConcepts.count() > 0) {

            val items = itemsWithMissingConcepts.flatMap(f => f.map(x => x)).collect().toMap;
            JobLogger.debug("Items with missing concepts - " + items.size, className)
            itemConcepts = items.map { x =>
                var contentId = "";
                if (gameIds.contains(x._2)) {
                    contentId = x._2;
                } else if (codeIdMap.contains(x._2)) {
                    contentId = codeIdMap.get(x._2).get;
                }
                (x._1, ItemAdapter.getItemConceptMaxScore(x._2, x._1, config.getOrElse("apiVersion", "v1").asInstanceOf[String]));
            }.toMap;
            JobLogger.debug("MC fetched from Item Model and broadcasting the data", className)
        }

        val itemConceptMapping = sc.broadcast(itemConcepts);
        val userSessions = filteredData.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b);

        JobLogger.debug("Calculating new Evidences per learner", className)
        val newEvidences = userSessions.mapValues { x =>
            val sortedEvents = x.sortBy { x => x.ets };
            val eventStartTimestamp = sortedEvents(0).syncts;
            val eventEndTimestamp = sortedEvents.last.syncts;

            val itemResponses = sortedEvents.map { x =>
                val gameId = x.dimensions.gdata.get.id
                val learner_id = x.uid.get
                x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemResponses").get.asInstanceOf[List[Map[String, AnyRef]]]
                    .map { f =>
                        val itemId = f.get("itemId").get.asInstanceOf[String];
                        val itemMC = getItemConcept(f, itemConceptMapping.value)
                        val itemMMC = f.getOrElse("mmc", List()).asInstanceOf[List[String]]
                        val score = f.get("score").get.asInstanceOf[Int]
                        val maxScore = getItemMaxScore(f, itemConceptMapping.value)
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

        JobLogger.debug("Joining new Evidences with previous learner state", className)
        val prevLearnerState = newEvidences.map { x => LearnerId(x._1) }.joinWithCassandraTable[LearnerProficiency](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFICIENCY_TABLE).map(f => (f._1.learner_id, f._2))
        JobLogger.warn("Previous learner state may be empty for the learners in new Evidences", className)

        JobLogger.debug("Calculating Learner Proficiency", className)
        val joinedRDD = newEvidences.leftOuterJoin(prevLearnerState);
        val lp = joinedRDD.mapValues(f => {

            val defaultAlpha = configMapping.value.getOrElse("alpha", 0.5d).asInstanceOf[Double];
            val defaultBeta = configMapping.value.getOrElse("beta", 1d).asInstanceOf[Double];
            val evidences = f._1
            val prevLearnerState = f._2.getOrElse(null);
            val prvProficiencyMap = if (null != prevLearnerState) prevLearnerState.proficiency else Map[String, Double]();
            val prvModelParams = if (null != prevLearnerState) prevLearnerState.model_params else Map[String, String]();
            val prvConceptModelParams = if (null != prevLearnerState && null != prevLearnerState.model_params) {
                prevLearnerState.model_params.map(f => {
                    val params = JSONUtils.deserialize[Map[String, Double]](f._2);
                    (f._1, (params.getOrElse("alpha", defaultAlpha), params.getOrElse("beta", defaultBeta)));
                });
            } else Map[String, (Double, Double)]();
            val startTime = new DateTime(evidences.last._3)
            val endTime = new DateTime(evidences.last._4)

            // Proficiency computation

            // Group by concepts and compute the sum of scores and maxscores
            val conceptScores = evidences.map(f => (f._2.itemMC, f._2.score, f._2.maxScore)).groupBy(f => f._1).mapValues(f => {
                (f.map(f => f._2).sum, f.map(f => f._3).sum);
            });

            // Compute the new alpha, beta and pi
            val conceptProfs = conceptScores.map { x =>
                val concept = x._1;
                val score = x._2._1;
                val maxScore = x._2._2;
                val N = maxScore
                val alpha = if (prvConceptModelParams.contains(concept)) prvConceptModelParams.get(concept).get._1 else defaultAlpha;
                val beta = if (prvConceptModelParams.contains(concept)) prvConceptModelParams.get(concept).get._2 else defaultBeta;
                val alphaNew = alpha + score;
                val betaNew = beta + N - score;
                val pi = (alphaNew / (alphaNew + betaNew));
                (concept, CommonUtil.roundDouble(alphaNew, 4), CommonUtil.roundDouble(betaNew, 4), CommonUtil.roundDouble(pi, 2)) // Pass updated (alpha, beta, pi) for each concept
            }

            // Update the previous learner state with new proficiencies and model params
            val newProfs = prvProficiencyMap ++ conceptProfs.map(f => (f._1, f._4)).toMap;
            val newModelParams = prvModelParams ++ conceptProfs.map(f => (f._1, JSONUtils.serialize(Map("alpha" -> f._2, "beta" -> f._3)))).toMap;

            (newProfs, startTime, endTime, newModelParams);
        }).map(f => {
            LearnerProficiency(f._1, f._2._1, f._2._2, f._2._3, f._2._4);
        }).cache();

        JobLogger.debug("Saving data to cassandra", className)
        lp.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFICIENCY_TABLE);
        JobLogger.info("LearnerProficiencySummary : execute method ending", className)
        lp.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEvent(userProf: LearnerProficiency, config: Map[String, AnyRef]): MeasuredEvent = {
        val mid = CommonUtil.getMessageId("ME_LEARNER_PROFICIENCY_SUMMARY", userProf.learner_id, "DAY", userProf.end_time.getMillis);
        val proficiencySummary = userProf.proficiency.map { x => ProficiencySummary(x._1, x._2) }
        val measures = Map("proficiencySummary" -> proficiencySummary)
        MeasuredEvent(None, "ME_LEARNER_PROFICIENCY_SUMMARY", System.currentTimeMillis(), userProf.end_time.getMillis, "1.0", mid, Option(userProf.learner_id), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ProficiencyUpdater").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", DtRange(userProf.start_time.getMillis, userProf.end_time.getMillis)),
            Dimensions(None, None, None, None, None, None, None),
            MEEdata(measures));
    }
}