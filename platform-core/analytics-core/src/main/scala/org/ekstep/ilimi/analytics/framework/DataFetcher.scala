package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * @author Santhosh
 */
object DataFetcher {

    def getBatchData(queries: Option[Array[Query]]): RDD[Event] = {
        null;
    }
    
    def getStreamData(query: Query): DStream[Event] = {
        null;
    }

}