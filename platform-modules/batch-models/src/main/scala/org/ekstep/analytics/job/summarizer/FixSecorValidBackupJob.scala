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

case class ValidEvent(`@timestamp`: AnyRef);
object FixSecorValidBackupJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.FixSecorValidBackupJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        implicit val sparkContext: SparkContext = sc.getOrElse(null);

        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        val data = DataFetcher.fetchBatchData[String](jobConfig.search).cache();

        val filterDate = CommonUtil.dateFormat.parseMillis("2017-12-20")
        val groupedData = data.map { event =>
            val syncts = JSONUtils.deserialize[ValidEvent](event).`@timestamp`;
            if (syncts.isInstanceOf[Double]) {
                (syncts.asInstanceOf[Double].toLong, event)
            } else if (syncts.isInstanceOf[Long]) {
                (syncts.asInstanceOf[Long], event)
            } else {
                (CommonUtil.getTimestamp(syncts.asInstanceOf[String]), event)
            }
        }.filter(f => f._1 >= filterDate).groupBy(f => CommonUtil.dateFormat.print(f._1));

        val filePrefix = "valid-corrected/"
        val bucket = "ekstep-dev-data-store"
        val out = groupedData.map { x =>
            val fileKey = filePrefix + x._1 + "-" + System.currentTimeMillis() + ".json.gz";
            val events = x._2.map(f => f._2);
            println("fileKey", fileKey)
            OutputDispatcher.dispatch(Dispatcher("s3file", Map("bucket" -> bucket, "key" -> fileKey, "file" -> fileKey, "zip" -> true.asInstanceOf[AnyRef])), events.toArray)
        }
        println("Input events count: ", data.count(), " Output events count: ", out.flatMap(f => f).count())
    }
}