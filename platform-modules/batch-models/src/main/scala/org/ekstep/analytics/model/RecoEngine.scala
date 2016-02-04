package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.adapter.ContentAdapter
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.updater.LearnerContentActivity
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.updater.ConceptSimilarity
import org.ekstep.analytics.framework.adapter.DomainAdapter
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.DtRange

case class PijMatrix(concept1: String, concept2: String, pijValue: Double);

case class Relevance(learner_id: String, relevance: Map[String, Double])

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        val sortedEvents = events.sortBy(x => x.ets).collect;
        val startTime = sortedEvents(0).ets
        val endTime = sortedEvents.last.ets

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", 0.33).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", 0.33).asInstanceOf[Double];
        val defaultWeightTimeSpent = configMapping.value.getOrElse("timeSpentWeight", 0.33).asInstanceOf[Double];
        val numOfIteration = configMapping.value.getOrElse("iterations", 3).asInstanceOf[Int];
        // getting Content-Concepts Map
        val contents = ContentAdapter.getAllContent();
        val contentConcepts = contents.map { x => (x.id, x.concepts) }.toMap;

        //getting all concept from Domain Model
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id }.distinct;
        //val conceptsBroadcast = sc.broadcast(concepts);

        //preparing concept-timespent map 
        val activitiesInDB = sc.cassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        val conceptTimeSpent = activitiesInDB.map { x => (x.learner_id, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { x =>
                val content = x.map { x => (contentConcepts.getOrElse(x.content_id, Array()), x.time_spent) }
                    .filter(f => (f._1.nonEmpty)).map { f =>
                        (f._1.map(a => (a, f._2)))
                    }.flatten.groupBy(a => a._1).map { f => (f._1, f._2.map(f => f._2).reduce((a, b) => a + b)) }
                content;
            }.collect.toMap;
        //val conceptTimeSpentBroadcast = sc.broadcast(conceptTimeSpent);

        //getting proficiency for each concept
        val proficiencies = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map { x => (x.learner_id, x.proficiency) }.collect().toMap;
        val proficienciesBroadcast = sc.broadcast(proficiencies);

        //getting concept similarity with default wight
        val similarities = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix").map { x => (x.concept1, x.concept2, x.relation_type, (defaultWeightSij) * (x.sim)) };
        val conceptSimilarities = sc.broadcast(similarities)

        // normalizing sij
        val normSimilarities = similarities.groupBy { x => x._1 }.map { f =>
            val row = f._2;
            val simTotal = f._2.map(a => a._4).sum
            row.map { x => (x._1, x._2, (x._4 / simTotal)) }.toBuffer;
        }.flatMap(f => f)
        val sijMap = normSimilarities.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap

        val learner = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map(x => x._1);

        // Pij & time spent computation

        //time spent computation
        val learnerTimeSpent = learner.map { x =>
            val timeSpentMap = conceptTimeSpent.getOrElse(x, Map());
            val totalTimeSpent = if (timeSpentMap.values.sum == 0) 1; else timeSpentMap.values.sum;
            (x, timeSpentMap.map(f => (f._1, (defaultWeightTimeSpent) * (f._2 / totalTimeSpent))))
        }.collect.toMap

        // pij computation
        val normPij = learner.map { x =>
            val proficiency = proficiencies.getOrElse(x, Map());
            val Pij = Map[String, Double]();
            concepts.foreach { a =>
                val PijRow = Buffer[PijMatrix]();
                var sum = 0d;
                concepts.foreach { b =>
                    val PijValue = (defaultWeightPij) * (Math.max(proficiency.getOrElse(a, 0.5d) - proficiency.getOrElse(b, 0.5d), 0.0001))
                    val roundValue = CommonUtil.roundDouble(PijValue, 2);
                    PijRow += PijMatrix(a, b, roundValue)
                    sum += roundValue
                }
                //Normalizing Pij values
                Pij ++ PijRow.map { x => (x.concept1 + "__" + x.concept2, (x.pijValue / sum)) }.toMap;
            }
            (x, Pij)
        }.collect().toMap;

        //val relevanceMap = concepts.map { x => (x, 0.33d) }
        val learnerRelevanceMap = sc.cassandraTable[Relevance]("learner_db", "conceptrelevance").map { x => (x.learner_id, x.relevance) }.collect().toMap;

        //matrix Addition - (pij + sij + normTimeSpent) & relevance calculation at 1 iteration 
        val learnerConceptRelevance = learner.map { x =>
            val pijMap = normPij.get(x).get
            val timeSpentMap = learnerTimeSpent.get(x).get
            var relevanceMap = learnerRelevanceMap.getOrElse(x, Map());
            var i = 0;
            for (i <- 1 to numOfIteration) {
                println("iteration start====>")
                val relevance = concepts.map { a =>
                    var rel = 0d;
                    concepts.map { b =>
                        val p = pijMap.getOrElse(a + "__" + b, 0.001d)
                        val s = sijMap.getOrElse(a + "__" + b, 0d)
                        val t = timeSpentMap.getOrElse(b, 0.01d)
                        val sigma = p + s + t;
                        rel += sigma * relevanceMap.getOrElse(b, 0.33);
                    }
                    (a, rel);
                }
                relevanceMap = relevance.toMap
            }
            Relevance(x, relevanceMap);
        }
        learnerConceptRelevance.saveToCassandra("learner_db", "conceptrelevance");
        learnerConceptRelevance.map(f => {
            getMeasuredEvent(f, configMapping.value, startTime, endTime);
        }).map { x => JSONUtils.serialize(x) };
    }
    private def getMeasuredEvent(userRel: Relevance, config: Map[String, AnyRef], startTime: Long, endTime: Long): MeasuredEvent = {
        val measures = userRel.relevance;
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_RELEVANCE_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(userRel.learner_id), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "RecoEngine").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "WEEK", DtRange(startTime, endTime)),
            Dimensions(None, None, None, None, None, None),
            MEEdata(measures));
    }
}