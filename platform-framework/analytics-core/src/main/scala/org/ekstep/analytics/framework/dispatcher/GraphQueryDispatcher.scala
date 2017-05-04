package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.streaming.GraphQuerySink
import org.ekstep.analytics.framework.exception.DispatcherException
import org.neo4j.driver.v1.StatementResult

object GraphQueryDispatcher {

	val config = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
		"user" -> AppConf.getConfig("neo4j.bolt.user"),
		"password" -> AppConf.getConfig("neo4j.bolt.password"));
	
	@throws(classOf[DispatcherException])
	def dispatch(queries: RDD[String])(implicit sc: SparkContext) = {
		val graphQuerySink = sc.broadcast(GraphQuerySink(config));
        queries.foreach { query => graphQuerySink.value.run(query) };
	}
	
	def dispatch(query: String)(implicit sc: SparkContext) : StatementResult = {
		val graphQuerySink = sc.broadcast(GraphQuerySink(config));
		graphQuerySink.value.run(query);
	}
}