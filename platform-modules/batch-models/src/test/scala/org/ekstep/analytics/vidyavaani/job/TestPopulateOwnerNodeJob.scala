package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j

class TestPopulateOwnerNodeJob extends SparkSpec(null) {
  
    it should "Clear all Owner Nodes in Neo4j" in {
        
        val neo = Neo4j(sc)
        val rel = neo.cypher("MATCH ()-[r:createdBy]-() DELETE r").loadRowRdd
        rel.count should be (0)
        val rdd = neo.cypher("MATCH (n:Owner) DELETE n").loadRowRdd
        rdd.count should be (0)
        
        PopulateOwnerNodeJob.main("{}")(Option(sc));
        
        
        val owner = neo.cypher("MATCH (n:Owner) Return n").loadRowRdd
        owner.count should be > (0L)
        val relations = neo.cypher("MATCH ()-[r:createdBy]-() Return r").loadRowRdd
        relations.count should be > (0L)      
    }
}