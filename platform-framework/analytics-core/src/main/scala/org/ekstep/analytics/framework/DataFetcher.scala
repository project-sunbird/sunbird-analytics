package org.ekstep.analytics.framework

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.ekstep.analytics.framework.fetcher.S3DataFetcher
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.ekstep.analytics.framework.util.CommonUtil
import org.apache.spark.streaming.StreamingContext
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
object DataFetcher {

    @throws(classOf[DataFetcherException])
    def fetchBatchData[T](search: Fetcher)(implicit mf:Manifest[T], sc: SparkContext): RDD[T] = {
        
        if(search.queries.isEmpty) {
            throw new DataFetcherException("Data fetch configuration not found");
        }
        val keys:Array[String] = search.`type`.toLowerCase() match {
            case "s3" =>
                Console.println("### Fetching the batch data from S3 ###");
                S3DataFetcher.getObjectKeys(search.queries.get).toArray;
            case "local" =>
                Console.println("### Fetching the batch data from Local file ###");
                search.queries.get.map { x => x.file.getOrElse("") }.filterNot { x => x == null };
            case _ =>
                throw new DataFetcherException("Unknown fetcher type found");
        }
        if (null == keys || keys.length == 0) {
            throw new DataFetcherException("No S3/Local Objects found for the qiven queries");
        }
        sc.textFile(keys.mkString(","), JobContext.parallelization).map { line =>
            {
                try {
                    JSONUtils.deserialize[T](line);   
                } catch {
                    case ex:Exception => 
                        null.asInstanceOf[T]
                }                    
            }
        }.filter { x => x != null }.cache();
    }

    /**
     * API to fetch the streaming data given an array of query objects
     */
    def fetchStreamData[T](sc: StreamingContext, search: Fetcher)(implicit mf:Manifest[T]): DStream[T] = {
        null;
    }
    
}