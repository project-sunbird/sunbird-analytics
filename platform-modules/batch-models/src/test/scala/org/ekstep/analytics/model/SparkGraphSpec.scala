package org.ekstep.analytics.model

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import java.io.File
import org.ekstep.analytics.framework.conf.AppConf
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants
import org.apache.commons.lang3.StringUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.util.DBUtil

/**
 * @author mahesh
 */

class SparkGraphSpec(override val file: String = "src/test/resources/sample_telemetry.log") extends SparkSpec(file) {

	var graphDb: GraphDatabaseService = null
	val testDataPath ="src/test/resources/vidyavaani-data/"

	override def beforeAll() {
		super.beforeAll();
		if (embeddedMode) {
			println("Starting Embedded Neo4j...");
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
			loadGraphData();
			// TODO: Remove this call after invoking this from the related Test file.
			loadCassandraData(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, testDataPath + "content_data.csv")
			
		}
	}

	override def afterAll() {
		super.afterAll();
		if (embeddedMode) {
			println("Stopping Embedded Neo4j...");
			if (null != graphDb) {
			    graphDb.execute("MATCH ()-[r:associatedTo]->() DELETE r")
			    graphDb.shutdown();
			}
		}
	}
	
	def executeQueries(queries: List[String]) {
        val tx = graphDb.beginTx();
        try {
            queries.map { query => graphDb.execute(query) };
            tx.success();
        } catch {
            case t: Throwable =>
                t.printStackTrace();
                tx.failure();
        } finally {
            tx.close();
        }
    }
	
	private def embeddedMode(): Boolean = {
		val isEmbedded = AppConf.getConfig("graph.service.embedded.enable");
		StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
	}
	
	def loadCassandraData(keyspace: String, table: String, file: String, text_delimiter: String = ",") {
	    DBUtil.importContentData(keyspace, table, file, text_delimiter);
	}
	
    def loadGraphData(file: String = testDataPath + "datanodes.json") {
        println("Preparing Test Graph using:"+ file);
        val nodes = sc.textFile(file, 1);
        val queries = List("MATCH (n) DETACH DELETE n") ++ nodes.map { x => s"CREATE (n:domain $x) return n" }.collect().toList ++
            List("MATCH (n: domain{IL_UNIQUE_ID:'org.ekstep.ra_ms_52d02eae69702d0905cf0800'}), (c: domain{IL_UNIQUE_ID:'Num:C1:SC1'}) CREATE (n)-[r:associatedTo]->(c) RETURN r");
        executeQueries(queries);
    }
}