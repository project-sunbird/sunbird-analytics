package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework.ConceptSimilarityJson
import org.ekstep.analytics.framework.util.JSONUtils

object ConceptSimilarityUpdater extends IBatchModel[ConceptSimilarityJson] with Serializable {

    case class ConceptSimilarity(concept1: String, concept2: String, relation_type: String, sim: Double);
    def execute(sc: SparkContext, jsonLines: RDD[ConceptSimilarityJson], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val similarity = jsonLines.map { x =>
            ConceptSimilarity(x.startNodeId, x.endNodeId, x.similarity.get("relationType").get.asInstanceOf[String], x.similarity.get("sim").get.asInstanceOf[Double]);
        }
        similarity.saveAsCassandraTable("learner_db", "conceptsimilaritymatrix");
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