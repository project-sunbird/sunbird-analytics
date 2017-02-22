package org.ekstep.analytics.util

import org.ekstep.analytics.framework.util.JSONUtils
import org.neo4j.spark.Neo4j
import org.ekstep.analytics.util.Neo4jQueryConstants._
object Neo4jUtil {

    def createNode(node: Node)(implicit neo4j: Neo4j) {
    	val props = removeKeyQuotes(JSONUtils.serialize(node.properties.getOrElse(Map())));
    	val createQuery = StringBuilder.newBuilder;
    	createQuery.append(CREATE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE)
    	.append(node.label).append(props).append(CLOSE_COMMON_BRACKETS)
        neo4j.cypher(createQuery.toString).loadRowRdd.collect
    }

    def deleteNodes(node: Node)(implicit neo: Neo4j) {
        val props = removeKeyQuotes(JSONUtils.serialize(node.properties.getOrElse(Map())));
        val deleteQuery = StringBuilder.newBuilder;
        deleteQuery.append(MATCH).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE)
        .append(node.label).append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
        .append(DETACH_DELETE).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);
        neo.cypher(deleteQuery.toString).loadRowRdd.collect
    }

    def addRelation(startNodeId: String, endNodeId: String, relation: String, relationProperties: Option[Map[String, AnyRef]] = None)(implicit neo: Neo4j) {
        
    }

    def deleteRelation(startNodeId: String, endNodeId: String, relation: String)(implicit neo: Neo4j) {
        
    }

    def deleteAllRelations(startNodeType: String, endNodeType: String, relation: String)(implicit neo: Neo4j) {

    }
    
    def removeKeyQuotes(query: String) : String = {
    	val regex = """"(\w+)":""";
    	return query.replaceAll(regex, "$1:")
    }
    	
}