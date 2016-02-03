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

//case class Relevance(concept: String, relevance: Double)

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", (1 / 3)).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", (1 / 3)).asInstanceOf[Double];
        val defaultWeightTimeSpent = configMapping.value.getOrElse("timeSpentWeight", (1 / 3)).asInstanceOf[Double];

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
        val Pij = Buffer[(String, String, Double)]();
        val learnerPij = learner.map { x =>
            val proficiency = proficiencies.getOrElse(x, Map());
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
                Pij ++ PijRow.map { x => (x.concept1, x.concept2, (x.pijValue / sum)) }
            }
            (x, Pij)
        };

        //matrix Addition - (pij + sij + normTimeSpent)  
        val sij = normSimilarities.map { x => (x._1 + "_" + x._2, x._3) }

        val learnerSigma = learnerPij.map { x =>
            
            val pij = learnerPij.map { f =>
                x._2.map { f => (f._1 + "__" + f._2, f._3) };
            }.flatMap(f => f)

            val pijSijJoin = pij.join(sij)
            val timeSpentNorm = learnerTimeSpent.get(x._1).get

            val sigma = pijSijJoin.map { x =>
                val concepts = x._1.split("__")
                (concepts(0), concepts(1), (timeSpentNorm.getOrElse(concepts(1), 0.01) + x._2._1 + x._2._2));
            }.collect
            (x._1,sigma);
        }

        //val relevaneMatrix = Buffer[(String,Double)](); 

        return null;
    }
}