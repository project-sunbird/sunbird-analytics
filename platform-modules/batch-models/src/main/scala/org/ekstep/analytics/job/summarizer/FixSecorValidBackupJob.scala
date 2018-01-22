package org.ekstep.analytics.job.summarizer

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher

object FixSecorValidBackupJob extends optional.Application with IJob {
      
    implicit val className = "org.ekstep.analytics.job.FixSecorValidBackupJob"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);
        
        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        val data = DataFetcher.fetchBatchData[Map[String, AnyRef]](jobConfig.search).cache();
        val groupedData = data.map { event =>
            val syncts = event.get("@timestamp").get.asInstanceOf[String]
            (syncts, Buffer(JSONUtils.serialize(event)))
        }.partitionBy(new HashPartitioner(JobContext.parallelization)).reduceByKey((a, b) => a ++ b)
        
        val filterDate = CommonUtil.dateFormat.parseMillis("2017-12-20")
        val filteredData = groupedData.filter(f => CommonUtil.df5.parseMillis(f._1) >= filterDate)
        
        val filePrefix = "valid-corrected/"
         val bucket = "ekstep-dev-data-store"
         val out = filteredData.map { x =>
             val longValue = CommonUtil.df5.parseMillis(x._1)
             val dateFormat = new DateTime(longValue).toLocalDate()
             val fileKey = filePrefix + dateFormat + "-" + longValue + ".json";
             println(fileKey)
             OutputDispatcher.dispatch(Dispatcher("s3", Map("bucket" -> bucket, "key" -> fileKey, "file" -> fileKey, "zip" -> true.asInstanceOf[AnyRef])), x._2.toArray)
        }
        println("Input events count: ", data.count(), " Output events count: ", out.flatMap(f => f).count())
    }
}