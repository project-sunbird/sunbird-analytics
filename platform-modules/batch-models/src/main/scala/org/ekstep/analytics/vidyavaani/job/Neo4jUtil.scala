package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.neo4j.spark.Neo4j

object Neo4jUtil {

    def createNode(label: String, properties: Map[String, AnyRef])(implicit neo: Neo4j) {
        
        val query = s"CREATE (n: $label"
        val script = if (null != properties) {
            val prop = JSONUtils.serialize(properties)
            s"$query $prop)"
        } else {
            s"$query)"
        }
        neo.cypher(script).loadRowRdd.collect
    }

    def deleteNode(label: String, properties: Map[String, AnyRef])(implicit neo: Neo4j) {
        val prop = JSONUtils.serialize(properties).replace("{\"", "{").replace(",\"", ",").replace("\":", ":")
        val query = s"MATCH (n: $label $prop) DELETE n"
        neo.cypher(query).loadRowRdd.collect
    }

    def addRelation(startNodeId: String, endNodeId: String, relation: String, relationProperties: Option[Map[String, AnyRef]] = None)(implicit neo: Neo4j) {

    }

    def deleteRelation(startNodeId: String, endNodeId: String, relation: String)(implicit neo: Neo4j) {

    }

    def deleteAllRelations(startNodeType: String, endNodeType: String, relation: String)(implicit neo: Neo4j) {

    }
}