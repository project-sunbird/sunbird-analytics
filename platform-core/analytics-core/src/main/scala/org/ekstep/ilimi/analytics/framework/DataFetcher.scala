package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.ilimi.analytics.framework.fetcher.S3DataFetcher
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.ekstep.ilimi.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object DataFetcher {

    @throws(classOf[DataFetcherException])
    def fetchBatchData(sc: SparkContext, queries: Option[Array[Query]], partitions: Int): RDD[Event] = {
        if (queries.nonEmpty) {
            val keys = S3DataFetcher.getObjectKeys(queries.get);
            if (null == keys || keys.length == 0) {
                throw new DataFetcherException("No S3 Objects found for the qiven queries");
            }
            sc.textFile(keys.mkString(","), partitions).map { line =>
                {
                    implicit val formats = DefaultFormats;
                    JsonMethods.parse(line).extract[Event]
                }
            }
        } else {
            throw new DataFetcherException("Data fetch configuration not found");
        }
    }

    /**
     * API to fetch the streaming data given an array of query objects
     */
    def fetchStreamData(query: Query): DStream[Event] = {
        null;
    }
    
}