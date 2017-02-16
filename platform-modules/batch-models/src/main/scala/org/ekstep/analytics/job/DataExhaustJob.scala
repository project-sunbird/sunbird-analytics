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
import org.ekstep.analytics.framework.DataExStage
import org.ekstep.analytics.model.JobResponse
import java.io.File

case class JobStage(request_id: String, client_key: String, stage: String, stage_status: String, status: String)

object DataExhaustJob extends optional.Application with IJob {

    implicit val className = "org.ekstep.analytics.job.DataExhaustJob"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {
        JobLogger.init("DataExhaustJob")
        JobLogger.start("DataExhaust Job Started executing", Option(Map("config" -> config)))
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
    }

    private def execute(config: JobConfig)(implicit sc: SparkContext) = {

        val modelParams = config.modelParams.get;
        val requests = getAllRequest
        if (null != requests) {
            val rdd = fetchAllData(requests, config, modelParams).cache
            _executeRequests(rdd.repartition(10), requests.collect(), modelParams);
            rdd.unpersist(true)
            requests.unpersist(true)
        } else {
            JobLogger.end("DataExhaust Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0)));
        }
    }

    def fetchAllData(requests: RDD[JobRequest], config: JobConfig, modelParams: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        try {
            val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd").withZoneUTC();
            val requestMap = requests.map { x =>
                {
                    val filter = JSONUtils.deserialize[RequestConfig](x.request_data).filter
                    (CommonUtil.dateFormat.parseDateTime(filter.start_date).withTimeAtStartOfDay().getMillis, CommonUtil.dateFormat.parseDateTime(filter.end_date).withTimeAtStartOfDay().getMillis)
                }
            };
            val startDate = requestMap.sortBy(f => f._1, true).first()._1;
            val endDate = requestMap.sortBy(f => f._2, false).first()._2;

            val fetcher = config.search.`type`.toLowerCase() match {
                case "s3" =>
                    val dataSetBucket = modelParams.get("dataset-read-bucket").getOrElse("ekstep-datasets").asInstanceOf[String];
                    val dataSetPrefix = modelParams.get("dataset-read-prefix").getOrElse("restricted/D001/4208ab995984d222b59299e5103d350a842d8d41/").asInstanceOf[String];
                    Fetcher(config.search.`type`, None, Option(Array(Query(Option(dataSetBucket), Option(dataSetPrefix), Option(dateFormat.print(startDate)), Option(dateFormat.print(endDate)), None, None, None, None, None, None, Option("aggregated-"), Option("yyyy/MM/dd")))));
                case "local" =>
                    Fetcher("local", None, config.search.queries);
            }
            val data = DataFetcher.fetchBatchData[String](fetcher);
            requests.map { x => JobStage(x.request_id, x.client_key, "FETCHING_DATA", "COMPLETED", "PROCESSING") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
            data;
        } catch {
            case t: Throwable =>
                requests.map { x => JobStage(x.request_id, x.client_key, "FETCHING_DATA", "FAILED", "FAILED") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
                throw t;
        }

    }
    def getAllRequest()(implicit sc: SparkContext): RDD[JobRequest] = {
        try {
            val jobReq = sc.cassandraTable[JobRequest](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST).filter { x => x.status.equals("SUBMITTED") }.cache;
            if (!jobReq.isEmpty()) {
                jobReq.map { x => JobStage(x.request_id, x.client_key, "FETCHING_ALL_REQUEST", "COMPLETED", "PROCESSING") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
                jobReq;
            } else {
                null;
            }

        } catch {
            case t: Throwable => null;
        }
    }

    private def downloadOutput(response: JobResponse, localPath: String)(implicit sc: SparkContext) {
        try {
            S3Util.download(response.bucket, response.prefix, localPath + "/")
            CommonUtil.deleteFile(localPath + "/" + response.request_id + "_$folder$")
            CommonUtil.deleteFile(localPath + "/_SUCCESS")
            CommonUtil.zipFolder(localPath + ".zip", localPath)
            CommonUtil.deleteDirectory(localPath);
            DataExhaustJobModel.updateStage(response.request_id, response.client_key, "DOWNLOAD_AND_ZIP_OUTPUT_FILE", "COMPLETED")
        } catch {
            case t: Throwable =>
                DataExhaustJobModel.updateStage(response.request_id, response.client_key, "DOWNLOAD_AND_ZIP_OUTPUT_FILE", "FAILED", "FAILED")
                throw t
        }
    }

    private def uploadZip(response: JobResponse, localPath: String)(implicit sc: SparkContext) {
        try {
            S3Util.uploadPublic(response.bucket, localPath + ".zip", response.prefix + ".zip");
            S3Util.deleteObject(response.bucket, response.prefix);
            S3Util.deleteObject(response.bucket, response.prefix + "_$folder$");
            CommonUtil.deleteFile(localPath + ".zip")
            DataExhaustJobModel.updateStage(response.request_id, response.client_key, "UPLOAD_ZIP", "COMPLETED")
        } catch {
            case t: Throwable =>
                DataExhaustJobModel.updateStage(response.request_id, response.client_key, "UPLOAD_ZIP", "FAILED", "FAILED")
                throw t
        }
    }

    private def _executeRequests(data: RDD[String], requests: Array[JobRequest], config: Map[String, AnyRef])(implicit sc: SparkContext) = {

        val inputEventsCount = data.count;
        val jobResponses = for (request <- requests) yield {

            try {
                val dt_processing = DateTime.now(DateTimeZone.UTC);
                val requestConfig = Map(
                    "request_id" -> request.request_id,
                    "client_key" -> request.client_key,
                    "job_id" -> UUID.randomUUID().toString(),
                    "data-exhaust-bucket" -> config.getOrElse("data-exhaust-bucket", "lpdev-ekstep"),
                    "data-exhaust-prefix" -> config.getOrElse("data-exhaust-prefix", "data-exhaust/dev"),
                    "dispatch-to" -> config.getOrElse("dispatch-to", "s3").asInstanceOf[String].toLowerCase(),
                    "path" -> config.getOrElse("tempLocalPath", "/tmp/dataexhaust"),
                    "fileType" -> request.file_type);


                val result = CommonUtil.time({
                    val response = DataExhaustJobModel.execute(data, Option(requestConfig)).collect().head;
                    val to = requestConfig.get("dispatch-to").get.asInstanceOf[String]
                    if (response.output_events > 0 && "s3".equals(to)) {
                        val localPath = requestConfig.get("path").get.asInstanceOf[String] + "/" + response.request_id;
                        downloadOutput(response, localPath)
                        uploadZip(response, localPath)
                    }
                    response
                })

                val jobReq = try {
                    if (result._2.output_events > 0) {

                        val fileDetail = requestConfig.get("dispatch-to").get match {
                            case "local" =>
                                val file = new File(result._2.prefix)
                                val dateTime = new Date(file.lastModified())
                                val stats = Map("createdDate" -> dateTime, "size" -> file.length())
                                (result._2.prefix, stats)
                            case "s3" =>
                                val stats = S3Util.getObjectDetails(result._2.bucket, result._2.prefix + ".zip");
                                val s3pubURL = config.getOrElse("data-exhaust-public-S3URL", "https://s3-ap-southeast-1.amazonaws.com").asInstanceOf[String]
                                val loc = s3pubURL + "/" + result._2.bucket + "/" + result._2.prefix + ".zip";
                                (loc, stats)
                        }

                        val createdDate = new DateTime(fileDetail._2.get("createdDate").get.asInstanceOf[Date].getTime);
                        JobRequest(request.file_type, request.client_key, request.request_id, Option(result._2.job_id), "COMPLETED", request.request_data, Option(fileDetail._1),
                            Option(new DateTime(createdDate)), Option(new DateTime(result._2.first_event_date)), Option(new DateTime(result._2.last_event_date)),
                            Option(createdDate.plusDays(30)), Option(0), request.dt_job_submitted, Option(dt_processing), Option(DateTime.now(DateTimeZone.UTC)),
                            Option(inputEventsCount), Option(result._2.output_events), Option(fileDetail._2.get("size").get.asInstanceOf[Long]), Option(0), Option(result._1), None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
                    } else {
                        JobRequest(request.file_type, request.client_key, request.request_id, Option(result._2.job_id), "COMPLETED", request.request_data, None,
                            None, Option(new DateTime(result._2.first_event_date)), Option(new DateTime(result._2.last_event_date)),
                            None, Option(0), request.dt_job_submitted, Option(dt_processing), Option(DateTime.now(DateTimeZone.UTC)),
                            Option(inputEventsCount), Option(result._2.output_events), None, Option(0), Option(result._1), None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
                    }
                } catch {
                    case t: Throwable =>
                        t.printStackTrace()
                        JobLogger.end("DataExhaust Job Completed.", "FAILED", Option(Map("date" -> "", "inputEvents" -> inputEventsCount)))
                        DataExhaustJobModel.updateStage(request.request_id, request.client_key, "UPDATE_RESPONSE_TO_DB", "FAILED", "FAILED")
                        null
                }
                jobReq;
            } catch {
                case t: Throwable =>
                    t.printStackTrace()
                    null
            }
        }
        val resRDD = sc.makeRDD(jobResponses.filter { x => null != x })
        try {
            resRDD.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST);
            JobLogger.end("DataExhaust Job Completed.", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> inputEventsCount, "outputEvents" -> 0, "timeTaken" -> 0)));
        } catch {
            case t: Throwable =>
                t.printStackTrace()
                JobLogger.end("DataExhaust Job Completed.", "FAILED", Option(Map("date" -> "", "inputEvents" -> inputEventsCount)))
                resRDD.map { x => JobStage(x.request_id, x.client_key, "UPDATE_RESPONSE_TO_DB", "FAILED", "FAILED") }.saveToCassandra(Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_REQUEST, SomeColumns("request_id", "client_key", "stage", "stage_status", "status"))
        }
    }
}
