package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
object SampleModel extends SessionBatchModel[Event] {
    
    def execute(sc: SparkContext, events: RDD[Event], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val gameSessions = getGameSessions(events);
        gameSessions.map(f => f._1);
    }
  
}

object SampleModelV2 extends SessionBatchModel[TelemetryEventV2] {
    
    def execute(sc: SparkContext, events: RDD[TelemetryEventV2], jobParams: Option[Map[String, AnyRef]]): RDD[String] = {
        val gameSessions = getGameSessionsV2(events);
        gameSessions.map(f => f._1);
    }
  
}