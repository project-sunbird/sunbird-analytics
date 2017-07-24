package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.conf.AppConf
import org.neo4j.driver.v1.Driver
import org.apache.commons.lang3.StringUtils
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.AuthTokens
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author mahesh
 */

trait IGraphExecutionModelTemplate extends IBatchModel[String, String] {

	val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
		"user" -> AppConf.getConfig("neo4j.bolt.user"),
		"password" -> AppConf.getConfig("neo4j.bolt.password"));

	/**
     * Override and implement the data product execute method. In addition to controlling the execution this base class records all generated RDD's,
     * so that they can be cleaned up from memory when necessary. This invokes all the three stages defined for a data product in the following order
     * 1. preProcess
     * 2. algorithm
     * 3. postProcess
     */
    override def execute(events: RDD[String], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        
        val config = jobParams.getOrElse(Map[String, AnyRef]());
        val inputRDD = preProcess(events, config);
        JobContext.recordRDD(inputRDD);
        val outputRDD = algorithm(inputRDD, config);
        JobContext.recordRDD(outputRDD);
        val resultRDD = postProcess(outputRDD, config);
        JobContext.recordRDD(resultRDD);
        resultRDD
    }

    /**
     * Pre processing steps before running the algorithm. Few pre-process steps are
     * 1. Transforming input - Filter/Map etc.
     * 2. Join/fetch data from LP
     * 3. Join/Fetch data from Cassandra
     * 4. Cleanup the data in Graph
     */
    def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String]

    /**
     * Method which runs the actual algorithm
     */
    def algorithm(queries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String]

    /**
     * Post processing on the algorithm output. Some of the post processing steps are
     * 1. Execute queries on Graph DB.
     * 
     */
	def postProcess(queries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
		executeQueries(queries)
		queries;
	}
	
	def executeQueries(queries: RDD[String])(implicit sc: SparkContext) {
		val driver = getDriver;
		val session = driver.session();
		val tx = session.beginTransaction();
		try {
			for (query <- queries.filter { x => StringUtils.isNotEmpty(x) }.collect()) {
				tx.run(query);
			}
			tx.success();
		} catch {
			case t: Throwable =>
				t.printStackTrace();
				tx.failure();
		} finally {
		    if (null != tx && tx.isOpen()) tx.close();
			if (null != session) session.close();
			if (null != driver) driver.close();
		}
	}

	private def getDriver(): Driver = {
		val isEmbedded = AppConf.getConfig("graph.service.embedded.enable");
		if (StringUtils.isNotBlank(isEmbedded) && StringUtils.equalsIgnoreCase("true", isEmbedded)) {
			val dbConfig = org.neo4j.driver.v1.Config.build().withEncryptionLevel(org.neo4j.driver.v1.Config.EncryptionLevel.NONE).toConfig();
			GraphDatabase.driver(graphDBConfig.getOrElse("url", "bolt://localhost:7687").asInstanceOf[String], dbConfig)
		} else {
			val authToken = AuthTokens.basic(graphDBConfig.getOrElse("user", "neo4j").asInstanceOf[String], graphDBConfig.getOrElse("password", "neo4j").asInstanceOf[String]);
			GraphDatabase.driver(graphDBConfig.getOrElse("url", "bolt://localhost:7687").asInstanceOf[String], authToken);
		}
	}
}