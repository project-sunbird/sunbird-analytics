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
import breeze.linalg._

case class Relevance(learner_id: String, relevance: Map[String, Double])

object RecoEngine extends IBatchModel[MeasuredEvent] with Serializable {

    def getConceptSimilarity(i: Int, j: Int, concepts: Array[String], conceptSimilarityMatrix: Map[String, Double]): Double = {
        conceptSimilarityMatrix.getOrElse(concepts(i) + "__" + concepts(j), 0.001d);
    }

    def normalizeMatrix(m: DenseMatrix[Double], j: DenseMatrix[Double]): DenseMatrix[Double] = {
        val x = m * j; // Row sums
        val y = j :/ x; // Create inverse of row sums
        m :* (y * j.t) // Normalization
    }

    def execute(sc: SparkContext, data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", 0.05).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", 0.05).asInstanceOf[Double];
        val defaultWeightTj = configMapping.value.getOrElse("timeSpentWeight", 0.9).asInstanceOf[Double];
        val iterations = configMapping.value.getOrElse("iterations", 3).asInstanceOf[Int];

        println("#### Fetching Content List and Domain Map ####")
        val contents = ContentAdapter.getAllContent();
        val conceptContentMap = contents.filterNot(x => (null == x.concepts || x.concepts.isEmpty)).map { x => (x.id, x.concepts) }.flatMap(f => f._2.map { x => (f._1, x) });
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id };
        val N = concepts.length;

        println("#### Broadcasting Content List and Domain Map ####")
        val conceptsData = sc.broadcast(concepts);
        val conceptContentMapping = sc.broadcast(conceptContentMap);

        println("conceptContentMap count", conceptContentMap.length);
        println("concepts count", concepts.length);
        /** Compute the Sij values - Start **/
        println("#### Fetching concept similarity matrix ####");
        val similarities = sc.cassandraTable[ConceptSimilarity]("learner_db", "conceptsimilaritymatrix")
            .map { x => (x.concept1, x.concept2, (x.sim)) }.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap;

        println("#### Normalizing the Concept Similarity matrix and broadcasting it to all nodes ####")
        val conceptSimilarities = DenseMatrix.zeros[Double](N, N);
        for (i <- 0 until conceptSimilarities.rows)
            for (j <- 0 until conceptSimilarities.cols)
                conceptSimilarities(i, j) = getConceptSimilarity(i, j, concepts, similarities);

        val Jt = DenseMatrix.fill[Double](N, 1) { 1.0 };
        val St = normalizeMatrix(conceptSimilarities, Jt);
        val conceptDistanceMapping = sc.broadcast(normalizeMatrix(St, Jt));
        val A = sc.broadcast(Jt);
        /** Compute the Sij values - End **/

        println("### Preparing learner date range map ###");
        // Get all learners date ranges
        val learnerDtRanges = data.map(event => (event.uid.get, Buffer[MeasuredEvent](event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val e = events.map { x => x.ets };
                DtRange(e.min, e.max);
            }.map { f => (LearnerId(f._1), f._2) };

        println("### Preparing all learners list ###");
        // Get all learners
        val allLearners = data.map(event => LearnerId(event.uid.get)).distinct;
        println("allLearners.count", allLearners.count);

        // Join all learners with learner content activity summary 
        val lcs = allLearners.joinWithCassandraTable[LearnerContentActivity]("learner_db", "learnercontentsummary").groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        // Join all learners with learner proficiency summaries
        val lp = allLearners.joinWithCassandraTable[LearnerProficiency]("learner_db", "learnerproficiency");

        // Join all learners with learner concept relevances
        val lcr = allLearners.joinWithCassandraTable[Relevance]("learner_db", "conceptrelevance");

        val learners = learnerDtRanges.leftOuterJoin(lp).map(f => (f._1, (f._2._1, f._2._2.getOrElse(LearnerProficiency(f._1.learner_id, Map(), DateTime.now(), DateTime.now(), Map())))))
            .leftOuterJoin(lcs).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2.getOrElse(Buffer[LearnerContentActivity]()))))
            .leftOuterJoin(lcr).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(Relevance(f._1.learner_id, Map())))));

        println("learners.count", learners.count);
        val learnerConceptRelevance = learners.map(learner => {

            val J = A.value;
            val C = conceptsData.value;
            val S = conceptDistanceMapping.value;
            val L = Array(defaultWeightSij, defaultWeightPij, defaultWeightTj);

            val dtRange = learner._2._1;
            val contentSummaries = learner._2._3;
            val learnerProficiency = learner._2._2;
            val learnerConceptRelevance = learner._2._4;
            val default = 1d / N;

            /** Compute the Tj values - Start **/
            val ts1 = System.currentTimeMillis();
            // 1. Compute timeSpent for each concept
            val contentTS = contentSummaries.map { x => (x.content_id, x.time_spent) }.toMap;
            val learnerConceptTS = conceptContentMapping.value.map(f => (f._2, contentTS.getOrElse(f._1, 0d))).groupBy(f => f._1).mapValues(f => f.map(x => x._2).sum);
            val conceptTS = conceptsData.value.map { x => (x, learnerConceptTS.getOrElse(x, default)) }.toMap;

            // 2. Total timeSpent on all concepts
            val totalTime = conceptTS.map(f => f._2).sum;

            // 3. Compute the Concept Tj Matrix
            val conceptTj = DenseMatrix.zeros[Double](1, N);
            for (i <- 0 until conceptTj.cols)
                conceptTj(0, i) = (conceptTS.get(conceptsData.value(i)).get) / totalTime

            val T = J * conceptTj;
            val ts2 = System.currentTimeMillis();
            /** Compute the Tj values - End **/

            /** Compute the Pij values - Start **/
            val proficiencyMap = learnerProficiency.proficiency;

            val conceptPi = DenseMatrix.tabulate(N, 1) { case (i, j) => proficiencyMap.getOrElse(C(i), 0d) };
            val cpj = conceptPi * J.t;
            val conceptP = cpj - cpj.t;
            val P = conceptP.map { x => if (x <= 0) 0.0001d else x }
            val Pn = normalizeMatrix(P, J);
            val ts3 = System.currentTimeMillis();
            /** Compute the Pij values - End **/

            /** Sigma Computations - Start **/
            val E = (L(0) * Pn) + (L(1) * S) + (L(2) * T);
            val ts4 = System.currentTimeMillis();
            /** Sigma Computations - End **/

            val random = new scala.util.Random;
            /** Compute the new concept relevancies - Start **/
            val lcr = learnerConceptRelevance.relevance;
            val B = DenseMatrix.zeros[Double](N, 1);
            for (i <- 0 until B.rows)
                B(i, 0) = lcr.getOrElse(conceptsData.value(i), random.nextDouble())
            
            var Rt = B :/ sum(B);
            for (1 <- 0 until iterations) {
                Rt = E * Rt;
            }
            /** Compute the new concept relevancies - End **/
            val ts5 = System.currentTimeMillis();

            val newConceptRelevance = Array.tabulate(N) { i => (conceptsData.value(i), Rt.valueAt(i)) }.toMap;
            //println("Learner Id", learner._1.learner_id, (ts2 - ts1), (ts3 - ts2), (ts4 - ts3), (ts5 - ts4));
            (learner._1.learner_id, newConceptRelevance, dtRange);
        }).cache();

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

    def main(args: Array[String]): Unit = {
        val t1 = System.currentTimeMillis()
        DenseMatrix.zeros[Double](333, 333);
        val t2 = System.currentTimeMillis();
        println("Time", (t2 - t1));
    }
}