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

case class PijMatrix(concept1: String, concept2: String, pijValue: Double);

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def execute(sc: SparkContext, events: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        // getting all concepts from concept model
        val contents = ContentAdapter.getAllContent();
        val contentConcepts = contents.map { x => (x.id, x.concepts) }.toMap;
        val concepts = contentConcepts.values.flatten.toBuffer.distinct;
        val conceptsBroadcast = sc.broadcast(concepts);

        //getting timespent for each content 
        val activitiesInDB = sc.cassandraTable[LearnerContentActivity]("learner_db", "learneractivity");
        val conceptTimeSpent = activitiesInDB.map { x => (x.learner_id, (contentConcepts.get(x.content_id).get, x.time_spent)) }
            .filter(f => (f._2._1.nonEmpty)).toArray.toMap.map { x =>
                (x._1, (x._2._1.map(f => (f, x._2._2))).toMap);
            };
        val conceptTimeSpentBroadcast = sc.broadcast(conceptTimeSpent);

        //getting proficiency for each concept
        val proficiencies = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map { x => (x.learner_id, x.proficiency) }.toArray().toMap;
        val proficienciesBroadcast = sc.broadcast(proficiencies);

        var Pij = Buffer[PijMatrix]();
        val learnerPij = events.map(event => (event.uid.get)).map { x =>
            val proficiency = proficiencies.get(x).get;
            val timeSpentMap = conceptTimeSpent.get(x).get
            val totalTimeSpent = timeSpentMap.values.sum;
            concepts.foreach { a =>
                concepts.foreach { b =>
                    val PijValue = Math.max(proficiency.getOrElse(a, 0.5d), proficiency.getOrElse(b, 0.5d)) + (timeSpentMap.getOrElse(b, 0d) / totalTimeSpent);
                    Pij += PijMatrix(a, b, PijValue)
                }
            }
            (x, Pij)
        };

        learnerPij.map { x => JSONUtils.serialize(x) };
    }
}