package org.ekstep.analytics.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.Filter

/**
 * @author Santhosh
 */
object SampleModel extends SessionBatchModel[Event,String] {
    
    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {
        val events = DataFilter.filter(data, Filter("eventId","IN",Option(List("OE_ASSESS","OE_START","OE_END","OE_LEVEL_SET","OE_INTERACT","OE_INTERRUPT"))));
        
        val modelMap = jobParams.getOrElse(Map())
        val model = modelMap.getOrElse("model","").asInstanceOf[String];
        val gameSessions = model match {
            case "GenieLaunch" =>
                getGenieLaunchSessions(data, 30)
            case "GenieSession" =>
                getGenieSessions(data, 30)
            case _ =>
                getGameSessions(events);
        }
        gameSessions.map(f => f._1);
    }
  
}