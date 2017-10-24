package org.ekstep.analytics.api.util

import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.fetcher.S3DataFetcher
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level._

object DataFetcher {
	implicit val className = "org.ekstep.analytics.api.util.DataFetcher"
  	@throws(classOf[DataFetcherException])
    def fetchBatchData[T](search: Fetcher)(implicit mf: Manifest[T], sc: SparkContext): RDD[T] = {

	    JobLogger.log("Fetching data", Option(Map("query" -> search)))
        if (search.queries.isEmpty) {
            throw new DataFetcherException("Data fetch configuration not found")
        }
        val date = search.queries.get.last.endDate
        val keys: Array[String] = search.`type`.toLowerCase() match {
            case "s3" =>
                JobLogger.log("Fetching the batch data from S3")
                S3DataFetcher.getObjectKeys(search.queries.get).toArray;
            case "local" =>
                JobLogger.log("Fetching the batch data from Local file")
                search.queries.get.map { x => x.file.getOrElse("") }.filterNot { x => x == null };
            case _ =>
                throw new DataFetcherException("Unknown fetcher type found");
        }
        if (null == keys || keys.length == 0) {
            return sc.parallelize(Seq[T](), JobContext.parallelization);
        }

        JobLogger.log("Deserializing Input Data", None, INFO);
        sc.textFile(keys.mkString(","), JobContext.parallelization).map { line =>
            {
                try {
                    val data = JSONUtils.deserialize[T](line);
                    println("Deserializing Done!!!");
                    println("data: "+ data)
                    data;
                } catch {
                    case ex: Exception =>
                        JobLogger.log(ex.getMessage, Option(Map("date" -> date)), INFO);
                        null.asInstanceOf[T]
                }
            }
        }.filter { x => x != null };
    }
}