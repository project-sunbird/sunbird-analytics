package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
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
import org.apache.log4j.Logger
import org.ekstep.analytics.updater.LearnerContentActivitySummary
import org.apache.spark.broadcast.Broadcast

case class LearnerConceptRelevance(learner_id: String, relevance: Map[String, Double])
case class LearnerState(learnerId: LearnerId, proficiency: LearnerProficiency, contentSummaries: Iterable[LearnerContentActivity], prevConceptRelevance: LearnerConceptRelevance, dtRange: DtRange) extends AlgoInput;
case class RelevanceScores(learnerId: LearnerId, relevanceScores: Map[String, Double], dtRange: DtRange) extends AlgoOutput;
case class RelevanceScore(conceptId: String, relevance: Double);

object RecommendationEngine extends IBatchModelTemplate[DerivedEvent, LearnerState, RelevanceScores, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.RecommendationEngine"

    var conceptsB: Broadcast[Array[String]] = null;
    var conceptToContentB: Broadcast[Array[(String, String)]] = null;

    /**
     * Function to aggregate data required to compute concept relevances for a learner.
     *
     * @param data - Session summaries which contain the information about learners for whom the relevance scores are re-computed
     * @param config - Job config which contains the model parameters
     *
     * @return RDD[LearnerState] - The current learner state
     */
    def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[LearnerState] = {

        JobLogger.log("Filtering ME_SESSION_SUMMARY events", className, None, None, None, "DEBUG")
        val filteredData = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")));
        val configMapping = sc.broadcast(config);

        JobLogger.log("Initializing lamda value for Pij, Sij, Tj", className, None, None, None, "DEBUG")
        
        JobLogger.log("Fetching Content List and Domain Map", className, None, None, None, "DEBUG")
        val contents = ContentAdapter.getAllContent();
        val concepts = DomainAdapter.getDomainMap().concepts.map { x => x.id };
        val conceptToContent = contents.filterNot(x => (null == x.concepts || x.concepts.isEmpty)).map { x => (x.id, x.concepts) }.flatMap(f => f._2.map { x => (f._1, x) });

        JobLogger.log("Content Coverage: " + conceptToContent.length, className, None, None, None, "DEBUG")
        JobLogger.log("Concept Count:" + concepts.length, className, None, None, None, "DEBUG")
        
        this.conceptsB = sc.broadcast(concepts);
        this.conceptToContentB = sc.broadcast(conceptToContent);

        JobLogger.log("Preparing Learner data", className, None, None, None, "DEBUG")
        // Get all learners date ranges
        val learnerDtRanges = filteredData.map(event => (event.uid, Buffer[DerivedEvent](event)))
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).mapValues { events =>
                val e = events.map { x => x.syncts };
                DtRange(e.min, e.max);
            }.map { f => (LearnerId(f._1), f._2) };

        JobLogger.log("Join learners with learner database", className, None, None, None, "DEBUG")
        // Get all learners
        val allLearners = filteredData.map(event => LearnerId(event.uid)).distinct;

        JobLogger.log("Join all learners with learner content activity summary", className, None, None, None, "DEBUG")
        // Join all learners with learner content activity summary 
        val lcs = allLearners.joinWithCassandraTable[LearnerContentActivity](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONTENT_SUMMARY_TABLE).groupBy(f => f._1).mapValues(f => f.map(x => x._2));
        JobLogger.log("LearnerContentActivity table may be empty", className, None, None, None, "WARN")
        
        JobLogger.log("Join all learners with learner proficiency summaries", className, None, None, None, "DEBUG")
        // Join all learners with learner proficiency summaries
        val lp = allLearners.joinWithCassandraTable[LearnerProficiency](Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFICIENCY_TABLE);
        JobLogger.log("LearnerProficiency table may be empty", className, None, None, None, "WARN")
        // Join all learners with learner concept relevances
        JobLogger.log("Join all learners with previous learner concept relevances", className, None, None, None, "DEBUG")
        val lcr = allLearners.joinWithCassandraTable[LearnerConceptRelevance](Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);
        JobLogger.log("There May be no concept relevance w.r.t some learner", className, None, None, None, "WARN")
        val learners = learnerDtRanges.leftOuterJoin(lp).map(f => (f._1, (f._2._1, f._2._2.getOrElse(LearnerProficiency(f._1.learner_id, Map(), DateTime.now(), DateTime.now(), Map())))))
            .leftOuterJoin(lcs).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._2.getOrElse(Buffer[LearnerContentActivity]()))))
            .leftOuterJoin(lcr).map(f => (f._1, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2.getOrElse(LearnerConceptRelevance(f._1.learner_id, Map())))));

        learners.map(f => {
            LearnerState(f._1, f._2._2, f._2._3, f._2._4, f._2._1);
        })
    }

    def algorithm(data: RDD[LearnerState], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[RelevanceScores] = {

        // Initialize Default variables from config
        val profWeight = config.getOrElse("profWeight", 0.33).asInstanceOf[Double];
        val conceptSimWeight = config.getOrElse("conSimWeight", 0.33).asInstanceOf[Double];
        val tsWeight = config.getOrElse("timeSpentWeight", 0.33).asInstanceOf[Double];
        val boostTsWeight = config.getOrElse("BoostTimeSpentWeight", 1.0).asInstanceOf[Double];
        val iterations = config.getOrElse("iterations", 5).asInstanceOf[Int];

        val dampingFactors = Array(conceptSimWeight, profWeight, tsWeight, boostTsWeight);
        val dampingFactorsNorm = dampingFactors.map { _ / dampingFactors.sum };

        // Broadcast required variables
        val noOfConcepts = this.conceptsB.value.length;
        val constMatrixB = sc.broadcast(DenseMatrix.fill[Double](noOfConcepts, 1) { 1.0 });
        val conceptSimMatrixB = sc.broadcast(conceptSimMatrix(conceptsB.value, constMatrixB.value, dampingFactorsNorm(0), noOfConcepts));

        data.map(learner => {

            val constMatrix = constMatrixB.value;
            val concepts = this.conceptsB.value;
            val conceptSimMatrix = conceptSimMatrixB.value;

            val dtRange = learner.dtRange;
            val contentSummaries = learner.contentSummaries;
            val learnerProficiency = learner.proficiency;
            val learnerConceptRelevance = learner.prevConceptRelevance.relevance;
            val default = 1d / noOfConcepts;

            val tsMatrix = timeSpentMatrix(contentSummaries, this.conceptToContentB.value, concepts, constMatrix, dampingFactorsNorm(2), noOfConcepts);
            val profMatrix = proficiencyMatrix(learnerProficiency, constMatrix, concepts, dampingFactorsNorm(1), noOfConcepts);
            val boostTsMatrix = boostTimeSpentMatrix(learnerProficiency, constMatrix, concepts, dampingFactorsNorm(3), noOfConcepts);
            val sigma = normalizeMatrix((profMatrix + conceptSimMatrix + boostTsMatrix + tsMatrix), constMatrix, 1.0d);

            val random = new scala.util.Random;
            val prevRelevanceMatrix = DenseMatrix.zeros[Double](1, noOfConcepts);

            var relevanceMatrix = prevRelevanceMatrix :/ sum(prevRelevanceMatrix);
            for (i <- 0 until iterations) {
                relevanceMatrix = relevanceMatrix * sigma;
            }
            val newConceptRelevance = Array.tabulate(noOfConcepts) { i => (concepts(i), relevanceMatrix.valueAt(i)) }.toMap;
            RelevanceScores(learner.learnerId, newConceptRelevance, learner.dtRange);
        }).cache();
    }

    def postProcess(data: RDD[RelevanceScores], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map(f => {
            LearnerConceptRelevance(f.learnerId.learner_id, f.relevanceScores);
        }).saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_CONCEPT_RELEVANCE_TABLE);

        data.map { learner =>
            val mid = CommonUtil.getMessageId("ME_LEARNER_CONCEPT_RELEVANCE", learner.learnerId.learner_id, "DAY", learner.dtRange.to);
            MeasuredEvent("ME_LEARNER_CONCEPT_RELEVANCE", System.currentTimeMillis(), learner.dtRange.to, "1.0", mid, learner.learnerId.learner_id, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "RecommendationEngine").asInstanceOf[String],
                    config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", learner.dtRange),
                Dimensions(None, None, None, None, None, None),
                MEEdata(Map("relevanceScores" -> learner.relevanceScores.map(f => RelevanceScore(f._1, f._2)))));
        }
    }

    /**
     * Function to return the concept similary matrix value
     */
    private def getConceptSimilarity(i: Int, j: Int, concepts: Array[String], conceptSimilarity: Map[String, Double]): Double = {
        conceptSimilarity.getOrElse(concepts(i) + "__" + concepts(j), 0.001d);
    }

    /**
     * Generic function to normalize matrix
     *
     * @param matrix - The NxN matrix to be normalized
     * @param constMatrix - The Nx1 constant matrix used for normalization. Usually it contains only 1.0
     * @param weigth - Lambda constant
     */
    private def normalizeMatrix(matrix: DenseMatrix[Double], constMatrix: DenseMatrix[Double], weight: Double): DenseMatrix[Double] = {
        JobLogger.log("Normalizing matrices", className, None, None, None, "DEBUG")
        val x = matrix * constMatrix; // Row sums
        // val y = constMatrix :/ x; // Create inverse of row sums
        weight * (matrix :/ (x * constMatrix.t)) // Normalization
    }

    /**
     * Function to compute the concept to concept similarity matrix
     *
     * @param concepts - Array of total concepts
     * @param constMatrix - The Nx1 constant matrix used for normalization. Usually it contains cells with value 1.0
     * @param conceptSimWeight - Lambda constant. Concept similarity weight to control the influence of concept to concept similarity on overall ranking
     * @param noOfConcepts - Total number of constants
     */
    private def conceptSimMatrix(concepts: Array[String], constMatrix: DenseMatrix[Double], conceptSimWeight: Double, noOfConcepts: Int)(implicit sc: SparkContext): DenseMatrix[Double] = {

        JobLogger.log("Computing the concept similarity matrix", className, None, None, None, "DEBUG")
        val conceptSimilarityMatrix = sc.cassandraTable[ConceptSimilarity](Constants.KEY_SPACE_NAME, Constants.CONCEPT_SIMILARITY_TABLE).map { x => (x.concept1, x.concept2, (x.sim)) }.map { x => (x._1 + "__" + x._2, x._3) }.collect.toMap;
        val conceptSimMatrix = DenseMatrix.zeros[Double](noOfConcepts, noOfConcepts);
        for (i <- 0 until conceptSimMatrix.rows)
            for (j <- 0 until conceptSimMatrix.cols)
                conceptSimMatrix(i, j) = getConceptSimilarity(i, j, concepts, conceptSimilarityMatrix);

        normalizeMatrix(conceptSimMatrix, constMatrix, conceptSimWeight);
    }

    /**
     * Function to compute the concept to timespent matrix
     *
     * @param contentSummaries - Content summary data
     * @conceptToContent - Content coverage in concept data
     * @param concepts - Array of total concepts
     * @param constMatrix - The Nx1 constant matrix used for normalization. Usually it contains cells with value 1.0
     * @param tsWeight - Lambda constant. Timespent weight to control the influence of timeSpent on concept in overall ranking
     * @param noOfConcepts - Total number of constants
     */
    private def timeSpentMatrix(contentSummaries: Iterable[LearnerContentActivity], conceptToContent: Array[(String, String)], concepts: Array[String], constMatrix: DenseMatrix[Double], tsWeight: Double, noOfConcepts: Int): DenseMatrix[Double] = {

        JobLogger.log("Computing the concept to time spent matrix", className, None, None, None, "DEBUG")
       val default = 1d / noOfConcepts;
        val contentTS = contentSummaries.map { x => (x.content_id, x.time_spent) }.toMap;
        val learnerConceptTS = conceptToContent.map(f => (f._2, contentTS.getOrElse(f._1, 0d))).groupBy(f => f._1).mapValues(f => f.map(x => x._2).sum);
        val conceptTS = concepts.map { x => (x, learnerConceptTS.getOrElse(x, default)) }.toMap;

        // 2. Total timeSpent on all concepts
        val totalTime = conceptTS.map(f => f._2).sum;

        // 3. Compute the Concept Tj Matrix
        JobLogger.log("Computing and normalizing Concept TimeSpent matrix (Tj)", className, None, None, None, "DEBUG")
        val conceptTsMatrix = DenseMatrix.zeros[Double](1, noOfConcepts);
        for (i <- 0 until conceptTsMatrix.cols)
            conceptTsMatrix(0, i) = (conceptTS.get(concepts(i)).get) / totalTime

        tsWeight * constMatrix * conceptTsMatrix;
    }

    /**
     * Function to compute the concept to proficiency matrix
     *
     * @param learnerProficiency - The current learner proficiency data
     * @param constMatrix - The Nx1 constant matrix used for normalization. Usually it contains cells with value 1.0
     * @param concepts - Array of total concepts
     * @param profWeight - Lambda constant. Proficiency weight to control the influence of concept proficiency in overall ranking
     * @param noOfConcepts - Total number of constants
     */
    private def proficiencyMatrix(learnerProficiency: LearnerProficiency, constMatrix: DenseMatrix[Double], concepts: Array[String], profWeight: Double, noOfConcepts: Int): DenseMatrix[Double] = {
        JobLogger.log("Computing the concept to proficiency matrix", className, None, None, None, "DEBUG")
        val proficiencyMap = learnerProficiency.proficiency;
        val conceptProfMatrix = DenseMatrix.tabulate(noOfConcepts, 1) { case (i, j) => proficiencyMap.getOrElse(concepts(i), 0d) };
        val cpj = conceptProfMatrix * constMatrix.t;
        val profMatrix = (cpj - cpj.t).map { x => if (x <= 0) 0.0001d else x };
        normalizeMatrix(profMatrix, constMatrix, profWeight);
    }

    /**
     * Function used to boost the concept time spent matrix.when content-to-concept coverage matrix is very sparse,
     * boost time_spent in a concept if certain assessment has taken place it assumes that learner has had some familiarity with the concept,
     * because of which an assessment is given in this concept.
     */
    def boostTimeSpentMatrix(learnerProficiency: LearnerProficiency, constMatrix: DenseMatrix[Double], concepts: Array[String], boostTsWeight: Double, noOfConcepts: Int): DenseMatrix[Double] = {

        val proficiencyMap = learnerProficiency.proficiency;
        val conceptPi = DenseMatrix.tabulate(noOfConcepts, 1) { case (i, j) => proficiencyMap.getOrElse(concepts(i), -1d) };
        val cpj = conceptPi * constMatrix.t;
        val profMatrix = cpj.t; //- cpj.t;
        val boostTijTmp = profMatrix.map { x => if (x > 0.0) 1d else x }
        val boostTij = boostTijTmp.map { x => if (x <= 0.5d) 0.01d else x }
        normalizeMatrix(boostTij, constMatrix, boostTsWeight);
    }

}