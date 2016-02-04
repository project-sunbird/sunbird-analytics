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
            .reduceByKey((a, b) => a ++ b).map { x =>
                val mapValue = x._2
                val content = mapValue.map { x => (contentConcepts.getOrElse(x.content_id, Array()), x.time_spent) }
                    .filter(f => (f._1.nonEmpty)).map { f =>
                        (f._1.map(a => (a, f._2)))
                    }.flatten.groupBy(a => a._1).map { f => (f._1, f._2.map(f => f._2).reduce((a, b) => a + b)) }
                (x._1, content);
            }.collect.toMap;

        //val conceptTimeSpentBroadcast = sc.broadcast(conceptTimeSpent);

        //getting proficiency for each concept
        val proficiencies = sc.cassandraTable[LearnerProficiency]("learner_db", "learnerproficiency").map { x => (x.learner_id, x.proficiency) }.collect().toMap;
        val proficienciesBroadcast = sc.broadcast(proficiencies);

        //getting concept similarity with default wight
        val similarities = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix")
            .map { x => (x.concept1, x.concept2, (defaultWeightSij) * (x.sim)) }.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap;

        // normalizing sij ####  Adding default sim value in Sij matrix new concept before normalization ####  
        val normSimilarities = concepts.map { c1 =>
            val row = concepts.map { c2 =>
                if (similarities.contains(c1 + "__" + c2)) {
                    (c1 + "__" + c2, similarities.get(c1 + "__" + c2).get);
                } else {
                    (c1 + "__" + c2, 0.001d);
                }
            }
            val simTotal = row.map(a => a._2).sum
            row.map { x => (x._1, (x._2 / simTotal)) };
        }.flatten.toMap;

        val learner = events.map(event => (event.uid.get, Buffer(event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map(x => x._1);

        // Pij & time spent computation
        //time spent computation
        // #### Adding default value to the conceptTimeSpent if concept is not there 
        // #### & Calculating default timeSpent matrix if it is a fresh learner  

        val value = (1.toDouble / concepts.length);
        val defaultMatrix = concepts.map { x => (x, value) }.toMap;
        val updatedconceptTimeSpent = conceptTimeSpent.map { x =>
            val learner = x._1
            val conceptTimeMap = x._2
            var totalTimeSpent = 0d
            if (conceptTimeMap.nonEmpty) totalTimeSpent = conceptTimeMap.values.sum; else totalTimeSpent = defaultMatrix.values.sum;
            val conceptTime = concepts.map { f =>
                if (!conceptTimeMap.contains(f)) {
                    (f, (value / totalTimeSpent));
                } else {
                    (f, (conceptTimeMap.get(f).get / totalTimeSpent));
                }
            }
            (learner, conceptTime.toMap);
        }

        //pij computation
        val normPij = learner.map { x =>
            val proficiency = proficiencies.getOrElse(x, Map());
            var Pij = Map[String, Double]();
            concepts.foreach { a =>
                var PijRow = Buffer[(String, String, Double)]();
                var sum = 0d;
                concepts.foreach { b =>
                    val PijValue = (defaultWeightPij) * (Math.max(proficiency.getOrElse(a, 0.5d) - proficiency.getOrElse(b, 0.5d), 0.0001))
                    PijRow.append((a, b, PijValue))
                    sum += PijValue
                }
                //Normalizing Pij values
                Pij = Pij ++ PijRow.map { x => (x._1 + "__" + x._2, (x._3 / sum)) }.toMap;
            }
            (x, Pij);
        }.collect().toMap;

        val learnerRelevanceMap = sc.cassandraTable[Relevance]("learner_db", "conceptrelevance").map { x => (x.learner_id, x.relevance) }.collect().toMap;

        //matrix Addition - (pij + sij + normTimeSpent) & relevance calculation at 1 iteration 
        val learnerConceptRelevance = learner.map { x =>
            val pijMap = normPij.get(x).get
            val timeSpentMap = updatedconceptTimeSpent.getOrElse(x, defaultMatrix)
            var relevanceMap = learnerRelevanceMap.getOrElse(x, defaultMatrix);
            for (i <- 1 to numOfIteration) {
                val relevance = concepts.map { a =>
                    var rel = 0d;
                    concepts.map { b =>
                        val p = pijMap.get(a + "__" + b).get
                        val s = normSimilarities.get(a + "__" + b).get
                        val t = timeSpentMap.get(b).get
                        val sigma = p + s + t;
                        rel += sigma * relevanceMap.getOrElse(b, value);
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