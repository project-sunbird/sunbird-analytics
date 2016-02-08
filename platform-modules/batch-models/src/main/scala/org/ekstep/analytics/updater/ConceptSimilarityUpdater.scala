package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.util.JSONUtils

case class ConceptSimilarity(concept1: String, concept2: String, relation_type: String, sim: Double)
case class ConceptSimilarityEntity(startNodeId: String, endNodeId: String, similarity: List[Map[String, AnyRef]])

object ConceptSimilarityUpdater extends IBatchModel[ConceptSimilarityEntity] with Serializable {

    def execute(sc: SparkContext, jsonLines: RDD[ConceptSimilarityEntity], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val similarity = jsonLines.map { x =>
            val similarity = x.similarity.last
            ConceptSimilarity(x.startNodeId, x.endNodeId, similarity.get("relationType").get.asInstanceOf[String], similarity.get("sim").get.asInstanceOf[Double]);
        }
        similarity.saveToCassandra("learner_db", "conceptsimilaritymatrix");
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