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

case class ContentData(content_id: String, body: Array[Byte], last_updated_on: Long, oldbody: Array[Byte]);

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
			importContentData(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, testDataPath + "content_data.csv")
			prepareTestGraph(graphDb);
		}
	}

	override def afterAll() {
		super.afterAll();
		if (embeddedMode) {
			println("Stopping Embedded Neo4j...");
			if (null != graphDb) graphDb.shutdown();
		}
	}
	
	
	private def embeddedMode(): Boolean = {
		val isEmbedded = AppConf.getConfig("graph.service.embedded.enable");
		StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded);
	}

	private def prepareTestGraph(graphDb: GraphDatabaseService) {
		println("Preparing Test Graph");
		val tx = graphDb.beginTx();
		try {
			graphDb.execute("MATCH (n) DETACH DELETE n");
			val nodes = sc.textFile(testDataPath + "datanodes.json", 1);
			val queries = nodes.map { x => s"CREATE (n:domain $x) return n" }.collect();
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
	
	private def importContentData(keyspace: String, table: String, file: String) {
         val rdd = sc.textFile(file)
         rdd.map { x => 
             val values = x.split(",")
             val body = values(1).getBytes
             ContentData(values(0), body, System.currentTimeMillis(), body)
         }.saveToCassandra(keyspace, table)
    }
	
}