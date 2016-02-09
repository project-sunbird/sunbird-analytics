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
import org.ekstep.analytics.framework.LearnerId
import org.ekstep.analytics.framework.DtRange
import org.joda.time.DateTime

case class Relevance(learner_id: String, relevance: Map[String, Double])

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {
    
    def execute(sc: SparkContext, data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", 0.33).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", 0.33).asInstanceOf[Double];
        val defaultWeightTj = configMapping.value.getOrElse("timeSpentWeight", 0.33).asInstanceOf[Double];
        val iterations = configMapping.value.getOrElse("iterations", 3).asInstanceOf[Int];
        
        println("#### Fetching Content List and Domain Map ####")
        val contents = ContentAdapter.getAllContent();
        val conceptContentMap = contents.filterNot(x => (null == x.concepts || x.concepts.isEmpty)).map { x => (x.id, x.concepts) }.flatMap(f => f._2.map { x => (f._1, x) });
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id };
        
        println("#### Broadcasting Content List and Domain Map ####")
        val conceptsData = sc.broadcast(concepts);
        val conceptContentMapping = sc.broadcast(conceptContentMap);
        
        println("conceptContentMap count", conceptContentMap.length);
        println("concepts count", concepts.length);
        /** Compute the Sij values - Start **/
        println("#### Fetching concept similarity matrix ####");
        val similarities = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix")
            .map { x => (x.concept1, x.concept2, (defaultWeightSij) * (x.sim)) }.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap;

        println("#### Normalizing the Concept Similarity matrix and broadcasting it to all nodes ####")
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
        val conceptDistanceMapping = sc.broadcast(normSimilarities);
        /** Compute the Sij values - End **/
        println("### Preparing learner date range map ###");
        // Get all learners date ranges
        val learnerDtRanges = data.map(event => (event.uid.get, Buffer[MeasuredEvent](event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val e = events.map { x => x.ets };
                DtRange(e.min, e.max);
            }.map {f => (LearnerId(f._1), f._2)};
        
        println("### Preparing all learners list ###");
        // Get all learners
        val allLearners = data.map(event => LearnerId(event.uid.get)).distinct;
        
        // Join all learners with learner content activity summary 
        val lcs = allLearners.joinWithCassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary").groupBy(f => f._1).mapValues(f => f.map(x => x._2));
        
        // Join all learners with learner proficiency summaries
        val lp = allLearners.joinWithCassandraTable[LearnerProficiency]("learner_db", "learnerproficiency");
        
        // Join all learners with learner concept relevances
        val lcr = allLearners.joinWithCassandraTable[Relevance]("learner_db", "conceptrelevance");
        
        val learners = learnerDtRanges.leftOuterJoin(lp).map(f => (f._1, (f._2._1, f._2._2.getOrElse(LearnerProficiency(f._1.learner_id, Map(), DateTime.now(), DateTime.now(), Map())))))
                                    .leftOuterJoin(lcs).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2.getOrElse(Buffer[LearnerContentActivity]()))))
                                    .leftOuterJoin(lcr).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(Relevance(f._1.learner_id, Map()))))).cache();
        println("learners count", learners.count());
        val learnerConceptRelevance = learners.map(learner => {
            val ts1 = System.currentTimeMillis();
            val dtRange = learner._2._1;
            val contentSummaries = learner._2._3;
            val learnerProficiency = learner._2._2;
            val learnerConceptRelevance = learner._2._4;
            val default = 1d/conceptsData.value.length;
            
            /** Compute the Tj values - Start **/
            // Compute timeSpent for each concept
            val contentTS = contentSummaries.map { x => (x.content_id, x.time_spent) }.toMap;
            val learnerConceptTS = conceptContentMapping.value.map(f => (f._2, contentTS.getOrElse(f._1, 0d))).groupBy(f => f._1).mapValues(f => f.map(x => x._2).sum);
            
            val conceptTS = conceptsData.value.map { x => (x, learnerConceptTS.getOrElse(x, default)) }
            // Total timeSpent on all concepts
            val T = conceptTS.map(f => f._2).sum;
            // Compute the Concept Tj map
            val conceptTj = conceptTS.map(f => (f._1, (defaultWeightTj * (f._2/T)))).toMap;
            /** Compute the Tj values - End **/
            
            /** Compute the Pij values - Start **/
            val proficiencyMap = learnerProficiency.proficiency;
            val conceptPij = conceptsData.value.map { a =>
                val row = conceptsData.value.map { b =>
                    (a, b, (defaultWeightPij) * (Math.max(proficiencyMap.getOrElse(a, 0d) - proficiencyMap.getOrElse(b, 0d), 0.0001)))
                }
                //Normalizing Pij values
                val rowSum = row.map(f => f._3).sum
                row.map { x => (x._1 + "__" + x._2, (x._3 / rowSum)) };
            }.flatMap(f => f).toMap
            /** Compute the Pij values - End **/
            
            /** Sigma Computations - Start **/
            val conceptSigma = conceptPij.map(f => {
                val concepts = f._1.split("__");
                val sij = conceptDistanceMapping.value.get(f._1).get;
                val pij = f._2;
                val tj = conceptTj.get(concepts(1)).get;
                val sigmaij = sij + pij + tj;
                (concepts(0), sigmaij)
            }).groupBy(f => f._1).mapValues(f => f.map(x => x._2).sum);
            /** Sigma Computations - End **/
            
            /** Compute the new concept relevancies - Start **/
            val lcr = learnerConceptRelevance.relevance;
            val conceptRelevance = conceptsData.value.map { x => (x, lcr.getOrElse(x, 1/default)) }
            val newConceptRelevance = conceptRelevance.map(f => {
                val sigma = conceptSigma.get(f._1).get;
                var relevance = f._2;
                for (i <- 1 to iterations) {
                    relevance *= sigma;
                }
                (f._1, relevance);
            }).toMap;
            /** Compute the new concept relevancies - End **/
            val ts2 = System.currentTimeMillis();
            //println("Learner Id", learner._1.learner_id, "Time taken to compute", (ts2-ts1));
            (learner._1.learner_id, newConceptRelevance, dtRange);
        });
        
        println("### Saving the data to Cassandra ###");
        /*learnerConceptRelevance.map(f => {
            Relevance(f._1, f._2)
        }).saveToCassandra("learner_db", "conceptrelevance");*/
        
        learnerConceptRelevance.map {
            f => getMeasuredEvent(f._1, f._2, configMapping.value, f._3);
        }.map { x => JSONUtils.serialize(x) };
        
    }
    
    private def getMeasuredEvent(uid: String, relevance: Map[String, Double], config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_RELEVANCE_SUMMARY").asInstanceOf[String], System.currentTimeMillis(), "1.0", Option(uid), None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "RecoEngine").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "WEEK", dtRange),
            Dimensions(None, None, None, None, None, None),
            MEEdata(relevance));
    }
}