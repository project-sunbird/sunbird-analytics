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

case class PijMatrix(concept1: String, concept2: String, pijValue: Double);

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        // getting all concepts from concept model
        val contents = ContentAdapter.getAllContent();
        val contentConcepts = contents.map { x => (x.id, x.concepts) }.toMap;
        val concepts = contentConcepts.values.flatten.toBuffer.distinct;
        val conceptsBroadcast = sc.broadcast(concepts);

        //getting timespent for each content 
        val activitiesInDB = sc.cassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary");
        val conceptTimeSpent = activitiesInDB.map { x => (x.learner_id, (contentConcepts.getOrElse(x.content_id, Array()), x.time_spent)) }
            .filter(f => (f._2._1.nonEmpty)).toArray.toMap.map { x =>
                (x._1, (x._2._1.map(f => (f, x._2._2))).toMap);
            };
        val conceptTimeSpentBroadcast = sc.broadcast(conceptTimeSpent);

        //getting proficiency for each concept
        val proficiencies = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map { x => (x.learner_id, x.proficiency) }.collect().toMap;
        val proficienciesBroadcast = sc.broadcast(proficiencies);

        //getting concept similarity
        val similarities = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix").map { x => (x.concept1, x.concept2, x.relation_type, x.sim) };
        val conceptSimilarities = sc.broadcast(similarities)
        
        println(concepts.length, "concepts count")
        println(events.collect().length, "events count")

        var Pij = Buffer[PijMatrix]();
        val learnerPij = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map(x => x._1).map { x =>
                val proficiency = proficiencies.getOrElse(x, Map());
                val timeSpentMap = conceptTimeSpent.getOrElse(x, Map());
                val totalTimeSpent = if (timeSpentMap.values.sum == 0)1;else timeSpentMap.values.sum;
                concepts.foreach { a =>
                    concepts.foreach { b =>
                        if (!a.equals(b)) {
                            val PijValue = Math.max(proficiency.getOrElse(a, 0.5d) - proficiency.getOrElse(b, 0.5d), 0) + ((timeSpentMap.getOrElse(b, 0d) / totalTimeSpent));
                            Pij += PijMatrix(a, b, CommonUtil.roundDouble(PijValue, 2))
                        }
                    }
                }
                (x, Pij)
            };
        learnerPij.map { x => JSONUtils.serialize(x) };
    }
}