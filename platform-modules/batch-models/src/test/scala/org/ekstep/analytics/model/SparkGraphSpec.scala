package org.ekstep.analytics.model

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import java.io.File
import org.ekstep.analytics.framework.conf.AppConf
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.Constants

class SparkGraphSpec(override val file: String = "src/test/resources/sample_telemetry.log") extends SparkSpec(file) {

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
        importCsvToCassandra(Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE, Array("content_id","body","last_updated_on","oldbody"), "src/test/resources/graph-db-neo4j/content_data.csv")
    }

    override def afterAll() {
        super.afterAll();
        println("Stop Embedded Neo4j...");
        if (null != graphDb) graphDb.shutdown();
    }
}