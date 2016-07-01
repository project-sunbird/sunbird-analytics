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
import org.apache.log4j.Logger
import org.ekstep.analytics.framework.util.JobLogger

/**
 * @author Santhosh
 */
object DataFetcher {

    val className = "org.ekstep.analytics.framework.DataFetcher"
    @throws(classOf[DataFetcherException])
    def fetchBatchData[T](search: Fetcher)(implicit mf: Manifest[T], sc: SparkContext): RDD[T] = {

        JobLogger.debug("Fetching data", className, Option(Map("query" -> search)))
        if (search.queries.isEmpty) {
            val exp = new DataFetcherException("Data fetch configuration not found")
            JobLogger.log(exp.getMessage, className, Option(exp), None, Option("FAILED"), "ERROR")
            throw exp;
        }
        val date = search.queries.get.last.endDate
        val keys: Array[String] = search.`type`.toLowerCase() match {
            case "s3" =>
                JobLogger.log("Fetching the batch data from S3", className, None, None, None, "DEBUG")
                S3DataFetcher.getObjectKeys(search.queries.get).toArray;
            case "local" =>
                JobLogger.log("Fetching the batch data from Local file", className, None, None, None, "DEBUG")
                search.queries.get.map { x => x.file.getOrElse("") }.filterNot { x => x == null };
            case _ =>
                val exp = new DataFetcherException("Unknown fetcher type found");
                JobLogger.log(exp.getMessage, className, Option(exp), Option(Map("date"->date)), Option("FAILED"), "ERROR")
                throw exp;
        }
        if (null == keys || keys.length == 0) {
            val exp = new DataFetcherException("No S3/Local Objects found for the qiven queries");
            JobLogger.log(exp.getMessage, className, Option(exp), Option(Map("date"->date)), Option("FAILED"), "ERROR")
            throw exp
        }
        JobLogger.log("Deserializing Input Data", className, None, None, None, "DEBUG")
        sc.textFile(keys.mkString(","), JobContext.parallelization).map { line =>
            {
                try {
                    JSONUtils.deserialize[T](line);
                } catch {
                    case ex: Exception =>
                        JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("date"->date)), Option("FAILED"), "ERROR")
                        null.asInstanceOf[T]
                }
            }
        }.filter { x => x != null }.cache();
    }

    /**
     * API to fetch the streaming data given an array of query objects
     */
    def fetchStreamData[T](sc: StreamingContext, search: Fetcher)(implicit mf: Manifest[T]): DStream[T] = {
        null;
    }

}