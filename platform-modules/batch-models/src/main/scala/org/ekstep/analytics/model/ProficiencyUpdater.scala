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

case class Assessment(learner_id: String, itemId: String, itemMC: List[String], itemMMC: List[String],
                      normScore: Double, maxScore: Int, itemMisconception: Array[String], timeSpent: Double);

case class LearnerProficiency(proficiency: Map[String, Double], startTime: Long, endTime: Long)

class ProficiencyUpdater extends IBatchModel[MeasuredEvent] with Serializable {
    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val assessments = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                var assessmentBuff = Buffer[Assessment]();
                val sortedEvents = x.sortBy { x => x.ts };
                val eventStartTimestamp = sortedEvents(0).ts;
                val eventEndTimestamp = sortedEvents.last.ts;
                sortedEvents.foreach { x =>
                    val learner_id = x.uid.get
                    val itemResponses = x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemResponses").get.asInstanceOf[List[Map[String, AnyRef]]]
                    itemResponses.foreach { f =>
                        val itemId = f.get("itemId").get.asInstanceOf[String];
                        val itemMC = f.getOrElse("mc", List()).asInstanceOf[List[String]]

                        if (itemMC.isEmpty && itemMC.length == 0) {
                            //fetch from content model
                        }

                        val itemMMC = f.getOrElse("mmc", List()).asInstanceOf[List[String]]
                        val score = f.get("score").get.asInstanceOf[Int]
                        val maxScore = f.getOrElse("maxScore", 0).asInstanceOf[Int]

                        if (maxScore == 0) {
                            //fetch from content model 
                        }
                        val timeSpent = f.get("timeSpent").get.asInstanceOf[Double]
                        val itemMisconception = Array[String]();
                        var normScore = 0d
                        if (maxScore != 0)
                            normScore = (score / maxScore);
                        val assess = Assessment(learner_id, itemId, itemMC, itemMMC, normScore, maxScore, itemMisconception, timeSpent)
                        assessmentBuff += assess
                    }
                }
                val model = assessmentBuff.map { x => (x.learner_id, x.itemMC.mkString(","), x.normScore, x.maxScore) }
                    .map { f =>
                        val alpha = UpdateProficiencyModelParam.getParameterAlpha(f._1, f._2).get
                        val beta = UpdateProficiencyModelParam.getParameterBeta(f._1, f._2).get
                        val X = Math.round((f._3 * f._4))
                        val N = f._4

                        val alphaNew = alpha + X;
                        val betaNew = beta + N - X;
                        val pi = (alphaNew / (alphaNew + betaNew));
                        (f._1, f._2, alphaNew, betaNew, pi);
                    }

                // saving model param to DB
                model.map(f => (f._1, f._2, f._3, f._4)).groupBy(f => f._2).map { f =>
                    val lastEvent = f._2.last
                    UpdateProficiencyModelParam.saveModelParamToDB(lastEvent._1, lastEvent._2, Option(lastEvent._3), Option(lastEvent._4))
                }

                var proficiencyMap = Map[String, Double]();
                val proficiency = model.map(x => (x._2, x._5)).groupBy(f => f._1).map { f =>
                    val lastEvent = f._2.last
                    val concept = lastEvent._1
                    val prof = lastEvent._2
                    proficiencyMap += (concept -> prof);
                }

                (LearnerProficiency(proficiencyMap, eventStartTimestamp, eventEndTimestamp), DtRange(eventStartTimestamp, eventEndTimestamp));
            }

        assessments.map(f => {
            getMeasuredEvent(f, configMapping.value);
        }).map { x => JSONUtils.serialize(x) };
    }
    private def getMeasuredEvent(userMap: (String, (LearnerProficiency, DtRange)), config: Map[String, AnyRef]): MeasuredEvent = {
        val measures = userMap._2._1;
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_PROFICIENCY_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userMap._1), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ProficiencyUpdater").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", userMap._2._2),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}