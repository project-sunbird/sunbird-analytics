package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger

case class ConceptSimilarity(concept1: String, concept2: String, relation_type: String, sim: Double)
case class ConceptSimilarityEntity(startNodeId: String, endNodeId: String, similarity: List[Map[String, AnyRef]])

object ConceptSimilarityUpdater extends IBatchModel[ConceptSimilarityEntity] with Serializable {

    val className = "org.ekstep.analytics.updater.ConceptSimilarityUpdater"
    def execute(jsonLines: RDD[ConceptSimilarityEntity], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        val similarity = jsonLines.map { x =>
            val similarity = x.similarity.last
            ConceptSimilarity(x.startNodeId, x.endNodeId, similarity.get("relationType").get.asInstanceOf[String], similarity.get("sim").get.asInstanceOf[Double]);
        }
        JobLogger.debug("Saving concept & similarity value to DB ", className)
        similarity.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.CONCEPT_SIMILARITY_TABLE);
        similarity.map { x =>
            val similarityMap = Map(
                "concept1" -> x.concept1,
                "concept2" -> x.concept2,
                "relation_type" -> x.relation_type,
                "similarity_value" -> x.sim);
            JSONUtils.serialize(similarityMap);
        };
    }
}