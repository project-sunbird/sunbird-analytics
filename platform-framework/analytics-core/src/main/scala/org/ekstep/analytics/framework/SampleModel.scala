package org.ekstep.analytics.framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
object SampleModel extends SessionBatchModel[Event] {
    
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        val events = DataFilter.filter(data, Filter("eventId","IN",Option(List("OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))));
        val gameSessions = getGameSessions(events);
        gameSessions.map(f => f._1);
    }
  
}

object SampleModelV2 extends SessionBatchModel[TelemetryEventV2] {
    
    def execute(data: RDD[TelemetryEventV2], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        val events = DataFilter.filter(data, Filter("eventId","IN",Option(List("OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))));
        val gameSessions = getGameSessionsV2(events);
        gameSessions.map(f => f._1);
    }
  
}