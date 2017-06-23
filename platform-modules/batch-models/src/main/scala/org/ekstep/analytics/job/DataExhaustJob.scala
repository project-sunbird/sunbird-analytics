package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobDriver
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.IJob
import org.ekstep.analytics.model.DataExhaustJobModel
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JSONUtils
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
import java.io.File
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.dataexhaust.DataExhaustUtils
import org.ekstep.analytics.util._

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

        val requests = DataExhaustUtils.getAllRequest
        if (null != requests) {
            _executeRequests(requests.collect(), config);
            requests.unpersist(true)
        } else {
            JobLogger.end("DataExhaust Job Completed. But There is no job request in DB", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> 0)));
        }
    }

    private def _executeRequests(requests: Array[JobRequest], config: JobConfig)(implicit sc: SparkContext) = {

        var inputEventsCount = 0l;
        val jobResponses = for (request <- requests) yield {

            try {
                val dtProcessing = DateTime.now(DateTimeZone.UTC);
                val requestData = JSONUtils.deserialize[RequestConfig](request.request_data);
                val datasetId = requestData.dataset_id.getOrElse("D002");
                val outputFormat = requestData.output_format.getOrElse("json").asInstanceOf[String]
                val updatedRequestData = JSONUtils.serialize(RequestConfig(requestData.filter, Option(datasetId), Option(outputFormat)))

                val modelConfig = config.modelParams.get
                val requestConfig = Map(
                    "request_id" -> request.request_id,
                    "client_key" -> request.client_key,
                    "job_id" -> UUID.randomUUID().toString(),
                    "data-exhaust-bucket" -> modelConfig.getOrElse("data-exhaust-bucket", "ekstep-public-dev"),
                    "data-exhaust-prefix" -> modelConfig.getOrElse("data-exhaust-prefix", "data-exhaust/test/"),
                    "dispatch-to" -> modelConfig.getOrElse("dispatch-to", "s3").asInstanceOf[String].toLowerCase(),
                    "path" -> modelConfig.getOrElse("tempLocalPath", "/tmp/dataexhaust"),
                    "output_format" -> outputFormat,
                    "dataset_id" -> requestData.dataset_id.get);

                val result = CommonUtil.time({
                    val data = DataExhaustUtils.fetchData(request, config)
                    inputEventsCount = inputEventsCount + data.count
                    val response = DataExhaustJobModel.execute(data, Option(requestConfig)).collect().head;
                    val to = requestConfig.get("dispatch-to").get.asInstanceOf[String]
                    if (response.output_events > 0 && "s3".equals(to)) {
                        val localPath = requestConfig.get("path").get.asInstanceOf[String] + "/" + response.request_id;
                        DataExhaustUtils.downloadOutput(response, localPath)
                        DataExhaustUtils.uploadZip(response, localPath)
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
                                val s3pubURL = modelConfig.getOrElse("data-exhaust-public-S3URL", "https://s3-ap-southeast-1.amazonaws.com").asInstanceOf[String]
                                val loc = s3pubURL + "/" + result._2.bucket + "/" + result._2.prefix + ".zip";
                                (loc, stats)
                        }

                        val createdDate = new DateTime(fileDetail._2.get("createdDate").get.asInstanceOf[Date].getTime);
                        JobRequest(request.client_key, request.request_id, Option(result._2.job_id), "COMPLETED", updatedRequestData, Option(fileDetail._1),
                            Option(new DateTime(createdDate)), Option(new DateTime(result._2.first_event_date)), Option(new DateTime(result._2.last_event_date)),
                            Option(createdDate.plusDays(30)), Option(request.iteration.getOrElse(0) + 1), request.dt_job_submitted, Option(dtProcessing), Option(DateTime.now(DateTimeZone.UTC)),
                            Option(inputEventsCount), Option(result._2.output_events), Option(fileDetail._2.get("size").get.asInstanceOf[Long]), Option(0), Option(result._1), None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
                    } else {
                        JobRequest(request.client_key, request.request_id, Option(result._2.job_id), "COMPLETED", updatedRequestData, None,
                            None, Option(new DateTime(result._2.first_event_date)), Option(new DateTime(result._2.last_event_date)),
                            None, Option(request.iteration.getOrElse(0) + 1), request.dt_job_submitted, Option(dtProcessing), Option(DateTime.now(DateTimeZone.UTC)),
                            Option(inputEventsCount), Option(result._2.output_events), None, Option(0), Option(result._1), None, Option("UPDATE_RESPONSE_TO_DB"), Option("COMPLETED"));
                    }
                } catch {
                    case t: Throwable =>
                        t.printStackTrace()
                        JobLogger.end("DataExhaust Job Completed.", "FAILED", Option(Map("date" -> "", "inputEvents" -> inputEventsCount)))
                        DataExhaustUtils.updateStage(request.request_id, request.client_key, "UPDATE_RESPONSE_TO_DB", "FAILED", "FAILED")
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