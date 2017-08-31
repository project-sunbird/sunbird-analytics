package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.JobResponse
import org.ekstep.analytics.api.JobOutput
import java.util.UUID
import org.ekstep.analytics.framework.util.JSONUtils
import scala.util.Random
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import akka.actor.Actor
import com.typesafe.config.Config
import akka.actor.Props
import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.Request
import org.ekstep.analytics.api.JobRequest
import java.security.MessageDigest
import scala.util.Sorting
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.JobStatus
import org.ekstep.analytics.api.JobStats
import org.ekstep.analytics.api.APIIds
import org.joda.time.DateTime
import org.ekstep.analytics.api.OutputFormat
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.S3Util
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest
import com.amazonaws.HttpMethod

/**
 * @author mahesh
 */

// TODO: Need to refactor the entire Service.
object JobAPIService {

    case class DataRequest(request: String, sc: SparkContext, config: Config);
    case class GetDataRequest(clientKey: String, requestId: String, sc: SparkContext, config: Config);
    case class DataRequestList(clientKey: String, limit: Int, sc: SparkContext, config: Config);

    case class ChannelData(datasetId: String, channel: String, from: String, to: String, sc: SparkContext, config: Config);

    def dataRequest(request: String)(implicit sc: SparkContext, config: Config): String = {
        val body = JSONUtils.deserialize[RequestBody](request);
        val isValid = _validateReq(body)
        if ("true".equals(isValid.get("status").get)) {
            val job = upsertRequest(body);
            val response = CommonUtil.caseClassToMap(_createJobResponse(job))
            JSONUtils.serialize(CommonUtil.OK(APIIds.DATA_REQUEST, response));
        } else {
            CommonUtil.errorResponseSerialized(APIIds.DATA_REQUEST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString())
        }
    }

    def getDataRequest(clientKey: String, requestId: String)(implicit sc: SparkContext, config: Config): String = {
        val job = DBUtil.getJobRequest(requestId, clientKey);
        if (null == job) {
            CommonUtil.errorResponseSerialized(APIIds.GET_DATA_REQUEST, "no job available with the given request_id and client_key", ResponseCode.CLIENT_ERROR.toString())
        } else {
            val jobStatusRes = _createJobResponse(job);
            JSONUtils.serialize(CommonUtil.OK(APIIds.GET_DATA_REQUEST, CommonUtil.caseClassToMap(jobStatusRes)));
        }
    }

    def getDataRequestList(clientKey: String, limit: Int)(implicit sc: SparkContext, config: Config): String = {
        val currDate = DateTime.now();
        val rdd = DBUtil.getJobRequestList(clientKey);
        val jobs = rdd.filter { f => f.dt_expiration.getOrElse(currDate).getMillis >= currDate.getMillis }
        val result = jobs.take(limit).map { x => _createJobResponse(x) }
        JSONUtils.serialize(CommonUtil.OK(APIIds.GET_DATA_REQUEST_LIST, Map("count" -> Long.box(jobs.count()), "jobs" -> result)));
    }

    def getChannelData(datasetId: String, channel: String, from: String, to: String)(implicit sc: SparkContext, config: Config): String = {

        val isValid = _validateChannelDataReqDtRange(datasetId, from, to)
        if ("true".equals(isValid.get("status").get)) {
            val bucket = config.getString("channel.data_exhaust.bucket")
            val expiry = config.getInt("channel.data_exhaust.expiryMins")
            val dates = org.ekstep.analytics.framework.util.CommonUtil.getDatesBetween(from, Option(to), "yyyy-MM-dd");

            val listObjs = for (date <- dates) yield {
                S3Util.getAllKeys(bucket, channel + "/" + date);
            }
            val objectKeys = listObjs.flatMap { x => x }

            if (objectKeys.length > 0) {
                val res = objectKeys.map { key => S3Util.getPreSignedURL(bucket, key, expiry) }
                JSONUtils.serialize(CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("telemetryURLs" -> res.map(x => x._1), "expiresAt" -> Long.box(res.map(x => x._2).last))));
            } else {
                JSONUtils.serialize(CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("telemetryURLs" -> Array(), "expiresAt" -> Long.box(0l))));
            }
        } else {
            CommonUtil.errorResponseSerialized(APIIds.CHANNEL_TELEMETRY_EXHAUST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString())
        }

    }

    private def upsertRequest(body: RequestBody)(implicit sc: SparkContext, config: Config): JobRequest = {
        val outputFormat = body.request.output_format.getOrElse(config.getString("data_exhaust.output_format"))
        val datasetId = body.request.dataset_id.getOrElse(config.getString("data_exhaust.dataset.default"));
        val requestId = _getRequestId(body.request.filter.get, outputFormat, datasetId, body.params.get.client_key.get);
        val job = DBUtil.getJobRequest(requestId, body.params.get.client_key.get);
        val usrReq = body.request;
        val request = Request(usrReq.filter, usrReq.summaries, usrReq.trend, usrReq.context, usrReq.query, usrReq.filters, usrReq.config, usrReq.limit, Option(outputFormat), Option(datasetId));
        if (null == job) {
            _saveJobRequest(requestId, body.params.get.client_key.get, request);
        } else {
            if (StringUtils.equalsIgnoreCase(JobStatus.FAILED.toString(), job.status.get)) {
                val retryLimit = config.getInt("data_exhaust.retry.limit");
                val attempts = job.iteration.getOrElse(0);
                if (attempts < retryLimit) _saveJobRequest(requestId, body.params.get.client_key.get, request, attempts); else job
            } else job
        }
    }

    private def _validateReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
        val params = body.params
        val filter = body.request.filter;
        val outputFormat = body.request.output_format.getOrElse(OutputFormat.JSON)
        if (filter.isEmpty || params.isEmpty) {
            val message = if (filter.isEmpty) "filter is empty" else "params is empty";
            Map("status" -> "false", "message" -> message);
        } else {
            val datasetList = config.getStringList("data_exhaust.dataset.list");
            if (outputFormat != null && !outputFormat.isEmpty && !(outputFormat.equals(OutputFormat.CSV) || outputFormat.equals(OutputFormat.JSON))) {
                Map("status" -> "false", "message" -> "invalid type. It should be one of [csv, json].");
            } else if (outputFormat != null && outputFormat.equals(OutputFormat.CSV) && (filter.get.events.isEmpty || !filter.get.events.get.length.equals(1))) {
                Map("status" -> "false", "message" -> "events should contains only one event.");
            } else if (filter.get.start_date.isEmpty || filter.get.end_date.isEmpty || params.get.client_key.isEmpty) {
                val message = if (params.get.client_key.isEmpty) "client_key is empty" else "start date or end date is empty"
                Map("status" -> "false", "message" -> message);
            } else if (filter.get.tags.nonEmpty && 0 == filter.get.tags.getOrElse(Array()).length) {
                Map("status" -> "false", "message" -> "tags are empty");
            } else if (!datasetList.contains(body.request.dataset_id.getOrElse(config.getString("data_exhaust.dataset.default")))) {
                val message = "invalid dataset_id. It should be one of " + datasetList;
                Map("status" -> "false", "message" -> message);
            } else {
                val endDate = filter.get.end_date.get
                val startDate = filter.get.start_date.get
                val days = CommonUtil.getDaysBetween(startDate, endDate)
                if (CommonUtil.getPeriod(endDate) >= CommonUtil.getPeriod(CommonUtil.getToday))
                    Map("status" -> "false", "message" -> "end_date should be lesser than today's date..");
                else if (0 > days)
                    Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your start_date & end_date");
                else if (30 < days)
                    Map("status" -> "false", "message" -> "Date range should be < 30 days");
                else Map("status" -> "true");
            }
        }
    }

    private def getDateInMillis(date: DateTime): Option[Long] = {
        if (null != date) Option(date.getMillis) else None
    }

    private def _createJobResponse(job: JobRequest): JobResponse = {
        val processed = List(JobStatus.COMPLETED.toString(), JobStatus.FAILED.toString()).contains(job.status.get);
        val created = if (job.dt_file_created.isEmpty) "" else job.dt_file_created.get.getMillis.toString()
        val output = if (processed) {
            val dfe = getDateInMillis(job.dt_first_event.getOrElse(null))
            val dle = getDateInMillis(job.dt_last_event.getOrElse(null))
            val de = getDateInMillis(job.dt_expiration.getOrElse(null))
            Option(JobOutput(job.location, job.file_size, Option(created), dfe, dle, de))
        } else Option(JobOutput());

        val djp = getDateInMillis(job.dt_job_processing.getOrElse(null))
        val djc = getDateInMillis(job.dt_job_completed.getOrElse(null))
        val stats = if (processed) {
            Option(JobStats(job.dt_job_submitted.get.getMillis, djp, djc, Option(job.input_events.getOrElse(0)), Option(job.output_events.getOrElse(0)), Option(job.latency.getOrElse(0)), Option(job.execution_time.getOrElse(0L))))
        } else Option(JobStats(job.dt_job_submitted.get.getMillis))
        val request = JSONUtils.deserialize[Request](job.request_data.getOrElse("{}"))
        val lastupdated = djc.getOrElse(djp.getOrElse(job.dt_job_submitted.get.getMillis));
        JobResponse(job.request_id.get, job.status.get, lastupdated, request, job.iteration.getOrElse(0), output, stats);
    }

    private def _saveJobRequest(requestId: String, clientKey: String, request: Request, iteration: Int = 0)(implicit sc: SparkContext): JobRequest = {
        val status = JobStatus.SUBMITTED.toString()
        val jobSubmitted = DateTime.now()
        val jobRequest = JobRequest(Option(clientKey), Option(requestId), None, Option(status), Option(JSONUtils.serialize(request)), Option(iteration), Option(jobSubmitted), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
        DBUtil.saveJobRequest(jobRequest);
        jobRequest;
    }

    private def _getRequestId(filter: Filter, outputFormat: String, datasetId: String, clientKey: String): String = {
        Sorting.quickSort(filter.tags.getOrElse(Array()));
        Sorting.quickSort(filter.events.getOrElse(Array()));
        val key = Array(filter.start_date.get, filter.end_date.get, filter.tags.getOrElse(Array()).mkString, filter.events.getOrElse(Array()).mkString, filter.app_id.getOrElse(""), filter.channel.getOrElse(""), outputFormat, datasetId, clientKey).mkString("|");
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }
    private def _validateChannelDataReqDtRange(datasetId: String, from: String, to: String): Map[String, String] = {

        if (StringUtils.isBlank(from)) {
            return Map("status" -> "false", "message" -> "Please provide 'from' in query string");
        }
        val days = CommonUtil.getDaysBetween(from, to)
        if (!StringUtils.equals("eks-consumption-raw", datasetId)) {
            return Map("status" -> "false", "message" -> "Please provide 'datasetId' as 'eks-consumption-raw' in your request URL");
        } else if (CommonUtil.getPeriod(to) > CommonUtil.getPeriod(CommonUtil.getToday))
            return Map("status" -> "false", "message" -> "'to' should be LESSER OR EQUAL TO today's date..");
        else if (0 > days)
            return Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your 'from' & 'to'");
        else if (10 < days)
            return Map("status" -> "false", "message" -> "Date range should be < 10 days");
        else return Map("status" -> "true");
    }
}

class JobAPIService extends Actor {
    import JobAPIService._;

    def receive = {
        case DataRequest(request: String, sc: SparkContext, config: Config) => sender() ! dataRequest(request)(sc, config);
        case GetDataRequest(clientKey: String, requestId: String, sc: SparkContext, config: Config) => sender() ! getDataRequest(clientKey, requestId)(sc, config);
        case DataRequestList(clientKey: String, limit: Int, sc: SparkContext, config: Config) => sender() ! getDataRequestList(clientKey, limit)(sc, config);
        case ChannelData(datasetId: String, channel: String, from: String, to: String, sc: SparkContext, config: Config) => sender() ! getChannelData(datasetId, channel, from, to)(sc, config);
    }
}