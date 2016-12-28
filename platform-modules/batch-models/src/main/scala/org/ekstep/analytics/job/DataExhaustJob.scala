package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.DataExhaustJobModel
import com.datastax.spark.connector._
import org.ekstep.analytics.model.JobRequest
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.RequestConfig
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.DataFetcher
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.ekstep.analytics.framework.util.S3Util
import org.joda.time.DateTime
import java.util.Date
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

object DataExhaustJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DataExhaustJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.log("Started executing Job")
        val jobConfig = JSONUtils.deserialize[JobConfig](config);
        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            try {
                execute(jobConfig);
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute(jobConfig);
        }
        JobLogger.log("Job Completed.")
    }

    private def execute(config: JobConfig)(implicit sc: SparkContext) = {

        val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd").withZoneUTC();
        val modelParams = config.modelParams.get;
        val requests = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).filter { x => x.status.equals("SUBMITTED") };
        val requestMap = requests.map { x =>
            {
                val filter = JSONUtils.deserialize[RequestConfig](x.request_data).filter
                (CommonUtil.dateFormat.parseDateTime(filter.start_date).withTimeAtStartOfDay().getMillis, CommonUtil.dateFormat.parseDateTime(filter.end_date).withTimeAtStartOfDay().getMillis)
            }
        };
        val startDate = requestMap.sortBy(f => f._1, true).first()._1;
        val endDate = requestMap.sortBy(f => f._2, false).first()._2;

        val dataSetBucket = modelParams.get("dataset-read-bucket").getOrElse("ekstep-datasets").asInstanceOf[String];
        val dataSetPrefix = modelParams.get("dataset-read-prefix").getOrElse("restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/").asInstanceOf[String];
        val fetcher = Fetcher(config.search.`type`, None, Option(Array(Query(Option(dataSetBucket), Option(dataSetPrefix), Option(dateFormat.print(startDate)), Option(dateFormat.print(endDate)), None, None, None, None, None, None, Option("aggregated-"), Option("yyyy/MM/dd")))));
        val rdd = DataFetcher.fetchBatchData[String](fetcher).cache();
        _executeRequests(rdd.repartition(10), requests.collect(), modelParams);
    }

    private def _executeRequests(data: RDD[String], requests: Array[JobRequest], config: Map[String, AnyRef])(implicit sc: SparkContext) = {

        val inputEventsCount = data.count;
        println("inputEventsCount", inputEventsCount);
        val jobResponses = for (request <- requests) yield {
            val dt_processing = DateTime.now(DateTimeZone.UTC);
            val requestConfig = Map(
                "request_id" -> request.request_id,
                "client_key" -> request.client_key,
                "job_id" -> UUID.randomUUID().toString(),
                "data-exhaust-bucket" -> config.getOrElse("data-exhaust-bucket", "lpdev-ekstep"),
                "data-exhaust-prefix" -> config.getOrElse("data-exhaust-bucket", "data-exhaust/dev"));

            val result = CommonUtil.time({
                val response = DataExhaustJobModel.execute(data, Option(requestConfig)).collect().head;
                println("response", response);
                val localPath = config.getOrElse("tempLocalPath", "/tmp/dataexhaust").asInstanceOf[String] + "/" + response.request_id;
                S3Util.download(response.bucket, response.prefix, localPath + "/")
                
                CommonUtil.deleteFile(localPath + "/" + response.request_id + "_$folder$")
                CommonUtil.deleteFile(localPath + "/_SUCCESS")
                CommonUtil.zipFolder(localPath + ".zip", localPath)
                CommonUtil.deleteDirectory(localPath);
                S3Util.uploadPublic(response.bucket, localPath + ".zip", response.prefix + ".zip");
                S3Util.deleteObject(response.bucket, response.prefix);
                S3Util.deleteObject(response.bucket, response.prefix + "_$folder$");
                CommonUtil.deleteFile(localPath + ".zip")
                response
            })

            val stats = S3Util.getObjectDetails(result._2.bucket, result._2.prefix + ".zip");
            val location = "https://" + "s3-ap-southeast-1.amazonaws.com/" + result._2.bucket + "/" + result._2.prefix + ".zip";
            val createdDate = new DateTime(stats.get("createdDate").get.asInstanceOf[Date].getTime);
            JobRequest(request.client_key, request.request_id, Option(result._2.job_id), "COMPLETED", request.request_data, Option(location),
                Option(new DateTime(createdDate)), Option(new DateTime(result._2.first_event_date)), Option(new DateTime(result._2.last_event_date)),
                Option(createdDate.plusDays(30)), Option(0), request.dt_job_submitted, Option(dt_processing), Option(DateTime.now(DateTimeZone.UTC)),
                Option(inputEventsCount), Option(result._2.output_events), Option(stats.get("size").get.asInstanceOf[Long]), Option(0), Option(result._1), None);
        }
        sc.makeRDD(jobResponses).saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST);
    }
    
}
