package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger

case class ConceptSimilarity(concept1: String, concept2: String, relation_type: String, sim: Double) extends AlgoOutput with Output
case class ConceptSimilarityEntity(startNodeId: String, endNodeId: String, similarity: List[Map[String, AnyRef]]) extends AlgoInput with Input

object ConceptSimilarityUpdater extends IBatchModelTemplate[ConceptSimilarityEntity, ConceptSimilarityEntity, ConceptSimilarity, ConceptSimilarity] with Serializable {

    val className = "org.ekstep.analytics.updater.ConceptSimilarityUpdater"

    override def preProcess(data: RDD[ConceptSimilarityEntity], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSimilarityEntity] = {
        data
    }

    override def algorithm(data: RDD[ConceptSimilarityEntity], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSimilarity] = {
        data.map { x =>
            x.similarity.map(f => {
                ConceptSimilarity(x.startNodeId, x.endNodeId, f.get("relationType").get.asInstanceOf[String], f.get("sim").get.asInstanceOf[Double])
            });
        }.flatMap { x => x.map { x => x } }
    }

    override def postProcess(data: RDD[ConceptSimilarity], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSimilarity] = {
        JobLogger.log("Saving concept & similarity value to DB", className, None, None, None)
        data.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.CONCEPT_SIMILARITY_TABLE);
        data
    }
}