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

case class PijMatrix(concept1: String, concept2: String, pijValue: Double);

case class Relevance(learner_id: String, concept_id: String, relevance: Double)

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", 0.33).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", 0.33).asInstanceOf[Double];
        val defaultWeightTimeSpent = configMapping.value.getOrElse("timeSpentWeight", 0.33).asInstanceOf[Double];

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
        val relevanceMap = sc.cassandraTable[Relevance]("learner_db", "conceptrelevance").map { x => (x.concept_id, x.relevance) }.collect().toMap;

        //matrix Addition - (pij + sij + normTimeSpent) & relevance calculation at 1 iteration 
        val learnerRelevance = learner.map { x =>
            val pijMap = normPij.get(x).get
            println(pijMap)
            val timeSpentMap = learnerTimeSpent.get(x).get
            val relevance = concepts.map { a =>
                var rel = 0d;
                concepts.map { b =>
                    val p = pijMap.getOrElse(a + "__" + b, 0.001d)
                    val s = sijMap.getOrElse(a + "__" + b, 0d)
                    val t = timeSpentMap.getOrElse(b, 0.01d)
                    val sigma = p + s + t;
                    rel += sigma * relevanceMap.getOrElse(b, 0.33);
                }
                Relevance(x, a, rel);
            }
            relevance;
        }.flatMap { x => x }

        learnerRelevance.saveToCassandra("learner_db", "conceptrelevance");
        learnerRelevance.map { x => JSONUtils.serialize(x) };
    }
}