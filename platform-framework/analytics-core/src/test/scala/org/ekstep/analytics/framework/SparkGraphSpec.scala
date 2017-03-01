package org.ekstep.analytics.framework

import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import java.io.File
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.driver.v1.Transaction
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.graphdb.Label
import java.util.concurrent.TimeUnit
import org.ekstep.analytics.framework.conf.AppConf

class SparkGraphSpec extends SparkSpec {
	
	var graphDb: GraphDatabaseService = null 
	
	override def beforeAll() {
        super.beforeAll();
        println("Start Embedded Neo4j...");
        val bolt = GraphDatabaseSettings.boltConnector("0");
		graphDb = new GraphDatabaseFactory()
			.newEmbeddedDatabaseBuilder(new File(AppConf.getConfig("graph.service.embedded.dbpath")))
			.setConfig(bolt.`type`, "BOLT")
			.setConfig(bolt.enabled, "true")
			.setConfig(bolt.address, "localhost:7687")
			.newGraphDatabase();
		sys.addShutdownHook {
			graphDb.shutdown();
		}
    }
	
    override def afterAll() {
        super.afterAll();
        println("Stop Embedded Neo4j...");
        if (null != graphDb) graphDb.shutdown();
    }
  
}