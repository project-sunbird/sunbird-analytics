package org.ekstep.analytics.util

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j

/**
 * @author mahesh
 */

class TestNeo4jUtil extends SparkSpec(null) {
	
	"Neo4jUtil" should "create a node" in {
		implicit val neo4j = Neo4j(sc); 
		val node = Node("User", Option(Map("name" -> "Analytics")));
		Neo4jUtil.createNode(node);
		// TODO: add assertion.
		
	}
	
	"Neo4jUtil" should "delete a node" in {
		implicit val neo4j = Neo4j(sc); 
		val node = Node("User");
		Neo4jUtil.deleteNodes(node);
		// TODO: add assertion.
	}
  
}