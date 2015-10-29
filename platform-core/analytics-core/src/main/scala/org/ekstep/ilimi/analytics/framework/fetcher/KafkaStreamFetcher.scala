package org.ekstep.ilimi.analytics.framework.fetcher

import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.ilimi.analytics.framework.Event;


/**
 * @author Santhosh
 */
object KafkaStreamFetcher {

    /*
 * Get a streaming window by time and sliding interval
 */
    def windowByTime(duration: Duration, slidingInterval: Duration, sc: StreamingContext, kafkaParams: Map[String, String]): DStream[Event] = {
        
        null;
    }

    /*
 * Get a streaming window by game session. This groups all events in a game session per user
 */
    def windowByGameSession(sc: StreamingContext, kafkaParams: Map[String, String]): DStream[Event] = {
        
        null;
    }

    /*
 * Get a streaming window by genie session. This groups all events in one genie session per user
 */
    def windowByGenieSession(sc: StreamingContext, kafkaParams: Map[String, String]): DStream[Event] = {
        
        null;
    }

}