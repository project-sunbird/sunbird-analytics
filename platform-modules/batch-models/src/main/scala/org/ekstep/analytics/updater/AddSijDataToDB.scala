package org.ekstep.analytics.updater

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object AddSijDataToDB {
    case class SijJson(startNodeId: String, endNodeId: String, similarity: Map[String, AnyRef]);
    case class Sij(concept1: String, concept2: String, relation_type: String, sji: Double);

    def writeToDb(sc: SparkContext, jsonLine: RDD[SijJson]) {

        val sij = jsonLine.map { x =>
            Sij(x.startNodeId, x.endNodeId, x.similarity.get("relationType").get.asInstanceOf[String], x.similarity.get("sim").get.asInstanceOf[Double]);
        }
    }
}