package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.ilimi.analytics.framework.fetcher.S3DataFetcher
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.apache.spark.streaming.StreamingContext

/**
 * @author Santhosh
 */
object DataFetcher {

    @throws(classOf[DataFetcherException])
    def fetchBatchData(sc: SparkContext, search: Fetcher): RDD[Event] = {
        if(search.queries.isEmpty) {
            throw new DataFetcherException("Data fetch configuration not found");
        }
        val keys:Array[String] = search.`type`.toLowerCase() match {
            case "s3" =>
                S3DataFetcher.getObjectKeys(search.queries.get).toArray;
            case "local" =>
                search.queries.get.map { x => x.file.getOrElse("") }.filterNot { x => x == null };
            case _ =>
                throw new DataFetcherException("Unknown fetcher type found");
        }
        if (null == keys || keys.length == 0) {
            throw new DataFetcherException("No S3/Local Objects found for the qiven queries");
        }
        sc.textFile(keys.mkString(","), JobContext.parallelization).map { line =>
            {
                implicit val formats = DefaultFormats;
                JsonMethods.parse(line).extract[Event]
            }
        }.cache();
    }

    /**
     * API to fetch the streaming data given an array of query objects
     */
    def fetchStreamData(sc: StreamingContext, search: Fetcher): DStream[Event] = {
        null;
    }
    
}