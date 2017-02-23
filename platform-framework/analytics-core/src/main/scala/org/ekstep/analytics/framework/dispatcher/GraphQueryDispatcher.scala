package org.ekstep.analytics.framework.dispatcher

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.streaming.GraphQuerySink
import org.ekstep.analytics.framework.exception.DispatcherException
import org.neo4j.driver.v1.StatementResult

object GraphQueryDispatcher  extends IDispatcher {

	@throws(classOf[DispatcherException])
	def dispatch(config: Map[String, AnyRef], queries: RDD[String])(implicit sc: SparkContext) = {
		val graphQuerySink = sc.broadcast(GraphQuerySink(config));
        queries.foreach { query => graphQuerySink.value.run(query) };
	}
	
	def dispatch(config: Map[String, String], query: String)(implicit sc: SparkContext) : StatementResult = {
		val graphQuerySink = sc.broadcast(GraphQuerySink(config));
		graphQuerySink.value.run(query);
	}
	
	def dispatch(queries: Array[String], config: Map[String, AnyRef]) : Array[String] = {
		Array();
	}
}