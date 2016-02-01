package org.ekstep.analytics.framework.fetcher

import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


/**
 * @author Santhosh
 */
object KafkaStreamFetcher {

    /*
 * Get a streaming window by time and sliding interval
 */
    def windowByTime[T](duration: Duration, slidingInterval: Duration, sc: StreamingContext, kafkaParams: Map[String, String])(implicit mf:Manifest[T]): DStream[T] = {
        
        null;
    }

    /*
 * Get a streaming window by game session. This groups all events in a game session per user
 */
    def windowByGameSession[T](sc: StreamingContext, kafkaParams: Map[String, String])(implicit mf:Manifest[T]): DStream[T] = {
        
        null;
    }

    /*
 * Get a streaming window by genie session. This groups all events in one genie session per user
 */
    def windowByGenieSession[T](sc: StreamingContext, kafkaParams: Map[String, String])(implicit mf:Manifest[T]): DStream[T] = {
        
        null;
    }

}