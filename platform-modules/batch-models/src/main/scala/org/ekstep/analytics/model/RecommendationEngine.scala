package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Filter
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
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JobLogger

case class LearnerConceptRelevance(learner_id: String, relevance: Map[String, Double])
case class RelevanceScores(conceptId: String, relevance: Double)

object RecommendationEngine extends IBatchModel[MeasuredEvent] with Serializable {
    
    val className = "org.ekstep.analytics.model.RecommendationEngine"

    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = configMapping.value.getOrElse("profWeight", 1.0).asInstanceOf[Double];
        val defaultWeightSij = configMapping.value.getOrElse("conSimWeight", 1.0).asInstanceOf[Double];
        val defaultWeightTj = configMapping.value.getOrElse("timeSpentWeight", 1.0).asInstanceOf[Double];
        val defaultWeightBoostTj = configMapping.value.getOrElse("BoostTimeSpentWeight", 1.0).asInstanceOf[Double];
        val iterations = configMapping.value.getOrElse("iterations", 20).asInstanceOf[Int];

        var L = Array(defaultWeightSij, defaultWeightPij, defaultWeightTj, defaultWeightBoostTj);
        //val Ltmp = L.map{_/L.sum};
        L = L.map { _ / L.sum };

        println("### ConSim Weight:" + defaultWeightSij + ":" + L(0) + " ###");
        println("### Prof Weight:" + defaultWeightPij + ":" + L(1) + " ###");
        println("### tsp Weight:" + defaultWeightTj + ":" + L(2) + " ###");
        println("### tsp Weight:" + defaultWeightBoostTj + ":" + L(3) + " ###");
        println("### iterations:" + iterations + " ###");

        println("#### Fetching Content List and Domain Map ####")

        val contents = ContentAdapter.getAllContent();
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id };
        val N = concepts.length;
        val Jn = DenseMatrix.fill[Double](N, 1) { 1.0 };
        val conceptContentMap = contents.filterNot(x => (null == x.concepts || x.concepts.isEmpty)).map { x => (x.id, x.concepts) }.flatMap(f => f._2.map { x => (f._1, x) });

        println("### Content Coverage:" + conceptContentMap.length + " ###");
        println("### Concept Count:" + concepts.length + " ###");

        println("#### Broadcasting all required data ####")
        val conceptsData = sc.broadcast(concepts);
        val conceptContentMapping = sc.broadcast(conceptContentMap);
        val jBroadcast = sc.broadcast(Jn);
        val SijBroadcast = sc.broadcast(computeSijMatrix(config, sc, concepts, Jn, L(0), N));

        println("### Preparing Learner data ###");
        // Get all learners date ranges
        val learnerDtRanges = data.map(event => (event.uid, Buffer[MeasuredEvent](event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val e = events.map { x => x.ets };
                DtRange(e.min, e.max);
            }.map { f => (LearnerId(f._1), f._2) };

        println("### Join learners with learner database ###");
        // Get all learners
        val allLearners = data.map(event => LearnerId(event.uid)).distinct;

        // Join all learners with learner content activity summary 
        val lcs = allLearners.joinWithCassandraTable[LearnerContentActivity](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONTENT_SUMMARY_TABLE).groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        // Join all learners with learner proficiency summaries
        val lp = allLearners.joinWithCassandraTable[LearnerProficiency](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFICIENCY_TABLE);

        // Join all learners with learner concept relevances
        val lcr = allLearners.joinWithCassandraTable[LearnerConceptRelevance](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);

        val learners = learnerDtRanges.leftOuterJoin(lp).map(f => (f._1, (f._2._1, f._2._2.getOrElse(LearnerProficiency(f._1.learner_id, Map(), DateTime.now(), DateTime.now(), Map())))))
            .leftOuterJoin(lcs).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2.getOrElse(Buffer[LearnerContentActivity]()))))
            .leftOuterJoin(lcr).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(LearnerConceptRelevance(f._1.learner_id, Map())))));

        val learnerConceptRelevance = learners.map(learner => {

            val J = jBroadcast.value;
            val C = conceptsData.value;
            val Sij = SijBroadcast.value;

            val dtRange = learner._2._1;
            val contentSummaries = learner._2._3;
            val learnerProficiency = learner._2._2;
            val learnerConceptRelevance = learner._2._4;
            val default = 1d / N;

            val Tij = computeTijMatrix(contentSummaries, conceptContentMapping.value, C, J, L(2), N);
            val Pij = computePijMatrix(learnerProficiency, J, C, L(1), N);
            val boostTij = boostTijMatrix(learnerProficiency, J, C, L(3), N);
            val EijTmp = Pij + Sij + boostTij + Tij;
            val Eij = normalizeMatrix(EijTmp, J, 1.0d);

            val random = new scala.util.Random;
            val lcr = learnerConceptRelevance.relevance;
            val r = DenseMatrix.zeros[Double](1, N);
            //      for (i <- 0 until r.rows)
            //        r(i, 0) = lcr.getOrElse(conceptsData.value(i), random.nextDouble())

            for (i <- 0 until r.rows)
                r(0, i) = 1

            var Rt = r :/ sum(r);
            for (i <- 0 until iterations) {

                Rt = Rt * Eij;
                //Rt = Rt:/sum(Rt);
            }

            val newConceptRelevance = Array.tabulate(N) { i => (C(i), Rt.valueAt(i)) }.toMap;
            (learner._1.learner_id, newConceptRelevance, dtRange);
        }).cache();

        println("### Saving the data to Cassandra ###");
        learnerConceptRelevance.map(f => {
            LearnerConceptRelevance(f._1, f._2)
        }).saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);

        println("### Creating summary events ###");
        learnerConceptRelevance.map { f =>
            val relevanceScores = (f._2).map { x => RelevanceScores(x._1, x._2) }
            getMeasuredEvent(f._1, relevanceScores, configMapping.value, f._3);
        }.map { x => JSONUtils.serialize(x) };

    }
    /**
     * Function to return the concept similary matrix value
     */
    def getConceptSimilarity(i: Int, j: Int, concepts: Array[String], conceptSimilarityMatrix: Map[String, Double]): Double = {
        conceptSimilarityMatrix.getOrElse(concepts(i) + "__" + concepts(j), 0.001d);
    }

    /**
     * Generic function to normalize matrix
     *
     * @param m - The NxN matrix to be normalized
     * @param j - The Nx1 constant matrix used for normalization. Usually it contains only 1.0
     * @param l - Lambda constant
     */
    def normalizeMatrix(m: DenseMatrix[Double], j: DenseMatrix[Double], l: Double): DenseMatrix[Double] = {
        val x = m * j; // Row sums
        //val y = j :/ x; // Create inverse of row sums
        l * (m :/ (x * j.t)) // Normalization
    }

    def computeSijMatrix(config: Map[String, AnyRef], sc: SparkContext, concepts: Array[String], Jn: DenseMatrix[Double], l: Double, n: Int): DenseMatrix[Double] = {

        val relationType = config.getOrElse("similarityType", "parentOf");
        val conceptSimilarityTable = sc.cassandraTable[ConceptSimilarity](Constants.KEY_SPACE_NAME, Constants.CONCEPT_SIMILARITY_TABLE);
        
        val conceptSimilarityMatrix = conceptSimilarityTable.filter(f => relationType.equals(f.relation_type)).map { x => (x.concept1, x.concept2, x.sim) }.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap;
        println("#### Normalizing the Concept Similarity matrix and broadcasting it to all nodes ####")
        val conceptSimilarities = DenseMatrix.zeros[Double](n, n);
        for (i <- 0 until conceptSimilarities.rows)
            for (j <- 0 until conceptSimilarities.cols)
                conceptSimilarities(i, j) = getConceptSimilarity(i, j, concepts, conceptSimilarityMatrix);

        normalizeMatrix(conceptSimilarities, Jn, l);
    }

    def computeTijMatrix(contentSummaries: Iterable[LearnerContentActivity], conceptContentMapping: Array[(String, String)], c: Array[String], j: DenseMatrix[Double], l: Double, n: Int): DenseMatrix[Double] = {
        val default = 1d / n;
        val contentTS = contentSummaries.map { x => (x.content_id, x.time_spent) }.toMap;
        val learnerConceptTS = conceptContentMapping.map(f => (f._2, contentTS.getOrElse(f._1, 0d))).groupBy(f => f._1).mapValues(f => f.map(x => x._2).sum);
        val conceptTS = c.map { x => (x, learnerConceptTS.getOrElse(x, default)) }.toMap;

        // 2. Total timeSpent on all concepts
        val totalTime = conceptTS.map(f => f._2).sum;
        //println(" -- Total time spent across all concepts:" + totalTime);

        // 3. Compute the Concept Tj Matrix
        val conceptTj = DenseMatrix.zeros[Double](1, n);
        for (i <- 0 until conceptTj.cols)
            conceptTj(0, i) = (conceptTS.get(c(i)).get) / totalTime

        l * j * conceptTj;
    }

    def computePijMatrix(learnerProficiency: LearnerProficiency, j: DenseMatrix[Double], c: Array[String], l: Double, n: Int): DenseMatrix[Double] = {

        val proficiencyMap = learnerProficiency.proficiency;
        val conceptPi = DenseMatrix.tabulate(n, 1) { case (i, j) => proficiencyMap.getOrElse(c(i), 0d) };
        val cpj = conceptPi * j.t;
        val profMatrix = cpj - cpj.t;
        val Pij = profMatrix.map { x => if (x <= 0) 0.05d else x }
        normalizeMatrix(Pij, j, l);
    }

    def boostTijMatrix(learnerProficiency: LearnerProficiency, j: DenseMatrix[Double], c: Array[String], l: Double, n: Int): DenseMatrix[Double] = {

        /* when content-to-concept coverge matrix is very sparse,
       boost time_spent in a concept if certain assessment has taken place
       it assumes that learner has had some familiarity with the concept, because of which an assessment is given
       in this concept */
        val proficiencyMap = learnerProficiency.proficiency;
        val conceptPi = DenseMatrix.tabulate(n, 1) { case (i, j) => proficiencyMap.getOrElse(c(i), -1d) };
        //println("concept pi"+conceptPi)
        val cpj = conceptPi * j.t;
        val profMatrix = cpj.t; //- cpj.t;
        val boostTijTmp = profMatrix.map { x => if (x > 0.0) 1d else x }
        val boostTij = boostTijTmp.map { x => if (x <= 0.5d) 0.01d else x }
        normalizeMatrix(boostTij, j, l);
    }

    def executeRetired(sc: SparkContext, data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {

        JobLogger.debug("Execute method started", className)
        JobLogger.debug("Filtering ME_SESSION_SUMMARY events", className)
        
        val filteredData = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val configMapping = sc.broadcast(config);

        // initializing lambda value 
        val defaultWeightPij = config.getOrElse("profWeight", 0.33).asInstanceOf[Double];
        val defaultWeightSij = config.getOrElse("conSimWeight", 0.33).asInstanceOf[Double];
        val defaultWeightTj = config.getOrElse("timeSpentWeight", 0.33).asInstanceOf[Double];
        val iterations = config.getOrElse("iterations", 3).asInstanceOf[Int];

        println("#### Fetching Content List and Domain Map ####")
        val contents = ContentAdapter.getAllContent();
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id };
        val N = concepts.length;
        val Jn = DenseMatrix.fill[Double](N, 1) { 1.0 };
        val conceptContentMap = contents.filterNot(x => (null == x.concepts || x.concepts.isEmpty)).map { x => (x.id, x.concepts) }.flatMap(f => f._2.map { x => (f._1, x) });

        println("### Content Coverage:" + conceptContentMap.length + " ###");
        println("### Concept Count:" + concepts.length + " ###");

        println("#### Broadcasting all required data ####")
        val conceptsData = sc.broadcast(concepts);
        val conceptContentMapping = sc.broadcast(conceptContentMap);
        val jBroadcast = sc.broadcast(Jn);
        val SijBroadcast = sc.broadcast(computeSijMatrix(config, sc, concepts, Jn, defaultWeightSij, N));

        println("### Preparing Learner data ###");
        // Get all learners date ranges
        val learnerDtRanges = filteredData.map(event => (event.uid, Buffer[MeasuredEvent](event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val e = events.map { x => x.ets };
                DtRange(e.min, e.max);
            }.map { f => (LearnerId(f._1), f._2) };

        println("### Join learners with learner database ###");
        // Get all learners
        val allLearners = filteredData.map(event => LearnerId(event.uid)).distinct;

        // Join all learners with learner content activity summary 
        val lcs = allLearners.joinWithCassandraTable[LearnerContentActivity](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONTENT_SUMMARY_TABLE).groupBy(f => f._1).mapValues(f => f.map(x => x._2));

        // Join all learners with learner proficiency summaries
        val lp = allLearners.joinWithCassandraTable[LearnerProficiency](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFICIENCY_TABLE);

        // Join all learners with learner concept relevances
        val lcr = allLearners.joinWithCassandraTable[LearnerConceptRelevance](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);

        val learners = learnerDtRanges.leftOuterJoin(lp).map(f => (f._1, (f._2._1, f._2._2.getOrElse(LearnerProficiency(f._1.learner_id, Map(), DateTime.now(), DateTime.now(), Map())))))
            .leftOuterJoin(lcs).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2.getOrElse(Buffer[LearnerContentActivity]()))))
            .leftOuterJoin(lcr).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(LearnerConceptRelevance(f._1.learner_id, Map())))));

        JobLogger.debug("Calculating Learner concept relevance", className)
        val learnerConceptRelevance = learners.map(learner => {

            val J = jBroadcast.value;
            val C = conceptsData.value;
            val Sij = SijBroadcast.value;
            val L = Array(defaultWeightSij, defaultWeightPij, defaultWeightTj);

            val dtRange = learner._2._1;
            val contentSummaries = learner._2._3;
            val learnerProficiency = learner._2._2;
            val learnerConceptRelevance = learner._2._4;
            val default = 1d / N;

            val Tij = computeTijMatrix(contentSummaries, conceptContentMapping.value, C, J, L(1), N);
            val Pij = computePijMatrix(learnerProficiency, J, C, L(2), N);
            val Eij = Pij + Sij + Tij;

            val random = new scala.util.Random;
            val lcr = learnerConceptRelevance.relevance;
            val r = DenseMatrix.zeros[Double](N, 1);
            for (i <- 0 until r.rows)
                r(i, 0) = lcr.getOrElse(conceptsData.value(i), random.nextDouble())

            var Rt = r :/ sum(r);
            for (i <- 0 until iterations) {
                Rt = Rt * Eij;
            }

            val newConceptRelevance = Array.tabulate(N) { i => (C(i), Rt.valueAt(i)) }.toMap;
            (learner._1.learner_id, newConceptRelevance, dtRange);
        }).cache();

        println("### Saving the data to Cassandra ###");
        learnerConceptRelevance.map(f => {
            LearnerConceptRelevance(f._1, f._2)
        }).saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);

        JobLogger.debug("Creating summary events", className)
        JobLogger.debug("Execute method ended", className)
        learnerConceptRelevance.map { f =>
            val relevanceScores = (f._2).map { x => RelevanceScores(x._1, x._2) }
            getMeasuredEvent(f._1, relevanceScores, configMapping.value, f._3);
        }.map { x => JSONUtils.serialize(x) };
    }

    private def getMeasuredEvent(uid: String, relevance: Iterable[RelevanceScores], config: Map[String, AnyRef], dtRange: DtRange): MeasuredEvent = {
        val mid = CommonUtil.getMessageId("ME_LEARNER_CONCEPT_RELEVANCE", uid, "DAY", dtRange);
        MeasuredEvent(config.getOrElse("eventId", "ME_LEARNER_CONCEPT_RELEVANCE").asInstanceOf[String], System.currentTimeMillis(), dtRange.to, "1.0", mid, uid, None, None,
            Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "RecommendationEngine").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", dtRange),
            Dimensions(None, None, None, None, None, None),
            MEEdata(Map("relevanceScores" -> relevance)));
    }
}