package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.{ APILogger, CommonUtil, DBUtil }
import org.ekstep.analytics.framework.util.JSONUtils
import akka.actor.Actor
import com.typesafe.config.Config
import org.ekstep.analytics.api._
import java.security.MessageDigest

import scala.util.Sorting
import org.ekstep.analytics.framework.JobStatus
import org.ekstep.analytics.api.JobStats
import org.ekstep.analytics.api.APIIds
import org.joda.time.DateTime
import org.ekstep.analytics.api.OutputFormat
import org.apache.commons.lang3.StringUtils
import java.util.Calendar

import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{ StorageConfig, StorageServiceFactory }

import scala.collection.mutable.Buffer

/**
 * @author mahesh
 */

// TODO: Need to refactor the entire Service.
object JobAPIService {

    implicit val className = "org.ekstep.analytics.api.service.JobAPIService"

    case class DataRequest(request: String, channel: String, config: Config)
    case class GetDataRequest(clientKey: String, requestId: String, config: Config)
    case class DataRequestList(clientKey: String, limit: Int, config: Config)

    case class ChannelData(channel: String, event_type: String, from: String, to: String, config: Config, code: Option[String])

    val EVENT_TYPES = Buffer("raw", "summary", "metrics", "failed")

    val storageType = AppConf.getStorageType()
    val storageService = StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageType), AppConf.getStorageSecret(storageType)))

    def dataRequest(request: String, channel: String)(implicit config: Config): Response = {
        val body = JSONUtils.deserialize[RequestBody](request)
        val isValid = _validateReq(body)
        if ("true".equals(isValid.get("status").get)) {
            val job = upsertRequest(body, channel)
            val response = CommonUtil.caseClassToMap(_createJobResponse(job))
            CommonUtil.OK(APIIds.DATA_REQUEST, response)
        } else {
            CommonUtil.errorResponse(APIIds.DATA_REQUEST, isValid.get("message").get, ResponseCode.CLIENT_ERROR.toString)
        }
    }

    def getDataRequest(clientKey: String, requestId: String)(implicit config: Config): Response = {
        val job = DBUtil.getJobRequest(requestId, clientKey)
        if (null == job) {
            CommonUtil.errorResponse(APIIds.GET_DATA_REQUEST, "no job available with the given request_id and client_key", ResponseCode.OK.toString)
        } else {
            val jobStatusRes = _createJobResponse(job)
            CommonUtil.OK(APIIds.GET_DATA_REQUEST, CommonUtil.caseClassToMap(jobStatusRes))
        }
    }

    def getDataRequestList(clientKey: String, limit: Int)(implicit config: Config): Response = {
        val currDate = DateTime.now()
        val jobRequests = DBUtil.getJobRequestList(clientKey)
        val jobs = jobRequests.filter { f => f.dt_expiration.getOrElse(currDate).getMillis >= currDate.getMillis }
        val result = jobs.take(limit).map { x => _createJobResponse(x) }
        CommonUtil.OK(APIIds.GET_DATA_REQUEST_LIST, Map("count" -> Int.box(jobs.size), "jobs" -> result))
    }

    def getChannelData(channel: String, eventType: String, from: String, to: String, code: Option[String] = Option(""))(implicit config: Config): Response = {

        val isValid = _validateRequest(channel, eventType, from, to)
        if ("true".equalsIgnoreCase(isValid.getOrElse("status", "false"))) {
            val bucket = config.getString("channel.data_exhaust.bucket")
            val basePrefix = config.getString("channel.data_exhaust.basePrefix")
            val expiry = config.getInt("channel.data_exhaust.expiryMins")
            val dates = org.ekstep.analytics.framework.util.CommonUtil.getDatesBetween(from, Option(to), "yyyy-MM-dd")

            val prefix =  if(code.nonEmpty && !StringUtils.equals(code.get, "wfs")) basePrefix + channel + "/" + eventType + "/" + code.get + "/" else basePrefix + channel + "/" + eventType + "/"

            val listObjs = storageService.searchObjectkeys(bucket, prefix, Option(from), Option(to), None)
            val calendar = Calendar.getInstance()
            calendar.add(Calendar.MINUTE, expiry)
            val expiryTime = calendar.getTime.getTime
            val expiryTimeInSeconds = expiryTime / 1000
            if (listObjs.length > 0) {
                val res = for (key <- listObjs) yield {
                    storageService.getSignedURL(bucket, key, Option(expiryTimeInSeconds.toInt))
                }
                CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("telemetryURLs" -> res, "expiresAt" -> Long.box(expiryTime)))
            } else {
                CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("telemetryURLs" -> Array(), "expiresAt" -> Long.box(0l)))
            }
        } else {
            APILogger.log("Request Validation FAILED")
            CommonUtil.errorResponse(APIIds.CHANNEL_TELEMETRY_EXHAUST, isValid.getOrElse("message", ""), ResponseCode.CLIENT_ERROR.toString)
        }
    }

    private def upsertRequest(body: RequestBody, channel: String)(implicit config: Config): JobRequest = {
        val outputFormat = body.request.output_format.getOrElse(config.getString("data_exhaust.output_format"))
        val datasetId = body.request.dataset_id.getOrElse(config.getString("data_exhaust.dataset.default"))
        val requestId = _getRequestId(body.request.filter.get, outputFormat, datasetId, body.params.get.client_key.get)
        val job = DBUtil.getJobRequest(requestId, body.params.get.client_key.get)
        val usrReq = body.request
        val useFilter = usrReq.filter.getOrElse(Filter(None, None, None, None, None, None, None, None, None, Option(channel)))
        val filter = Filter(None, None, None, useFilter.tag, useFilter.tags, useFilter.start_date, useFilter.end_date, useFilter.events, useFilter.app_id, Option(channel))
        val request = Request(Option(filter), usrReq.summaries, usrReq.trend, usrReq.context, usrReq.query, usrReq.filters, usrReq.config, usrReq.limit, Option(outputFormat), Option(datasetId))

        if (null == job) {
            _saveJobRequest(requestId, body.params.get.client_key.get, request)
        } else {
            if (StringUtils.equalsIgnoreCase(JobStatus.FAILED.toString(), job.status.get)) {
                val retryLimit = config.getInt("data_exhaust.retry.limit")
                val attempts = job.iteration.getOrElse(0)
                if (attempts < retryLimit) _saveJobRequest(requestId, body.params.get.client_key.get, request, attempts) else job
            } else job
        }
    }

    private def _validateReq(body: RequestBody)(implicit config: Config): Map[String, String] = {
        val params = body.params
        val filter = body.request.filter
        val outputFormat = body.request.output_format.getOrElse(OutputFormat.JSON)
        if (filter.isEmpty || params.isEmpty) {
            val message = if (filter.isEmpty) "filter is empty" else "params is empty"
            Map("status" -> "false", "message" -> message)
        } else {
            val datasetList = config.getStringList("data_exhaust.dataset.list")
            if (outputFormat != null && !outputFormat.isEmpty && !(outputFormat.equals(OutputFormat.CSV) || outputFormat.equals(OutputFormat.JSON))) {
                Map("status" -> "false", "message" -> "invalid type. It should be one of [csv, json].")
            } else if (outputFormat != null && outputFormat.equals(OutputFormat.CSV) && (filter.get.events.isEmpty || !filter.get.events.get.length.equals(1))) {
                Map("status" -> "false", "message" -> "events should contains only one event.")
            } else if (filter.get.start_date.isEmpty || filter.get.end_date.isEmpty || params.get.client_key.isEmpty) {
                val message = if (params.get.client_key.isEmpty) "client_key is empty" else "start date or end date is empty"
                Map("status" -> "false", "message" -> message)
            } else if (filter.get.tags.nonEmpty && 0 == filter.get.tags.getOrElse(Array()).length) {
                Map("status" -> "false", "message" -> "tags are empty")
            } else if (!datasetList.contains(body.request.dataset_id.getOrElse(config.getString("data_exhaust.dataset.default")))) {
                val message = "invalid dataset_id. It should be one of " + datasetList
                Map("status" -> "false", "message" -> message)
            } else {
                val endDate = filter.get.end_date.get
                val startDate = filter.get.start_date.get
                val days = CommonUtil.getDaysBetween(startDate, endDate)
                if (CommonUtil.getPeriod(endDate) >= CommonUtil.getPeriod(CommonUtil.getToday))
                    Map("status" -> "false", "message" -> "end_date should be lesser than today's date..")
                else if (0 > days)
                    Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your start_date & end_date")
                else if (30 < days)
                    Map("status" -> "false", "message" -> "Date range should be < 30 days")
                else Map("status" -> "true")
            }
        }
    }

    private def getDateInMillis(date: DateTime): Option[Long] = {
        if (null != date) Option(date.getMillis) else None
    }

    private def _createJobResponse(job: JobRequest): JobResponse = {
        val processed = List(JobStatus.COMPLETED.toString(), JobStatus.FAILED.toString).contains(job.status.get)
        val created = if (job.dt_file_created.isEmpty) "" else job.dt_file_created.get.getMillis.toString
        val output = if (processed) {
            val dfe = getDateInMillis(job.dt_first_event.getOrElse(null))
            val dle = getDateInMillis(job.dt_last_event.getOrElse(null))
            val de = getDateInMillis(job.dt_expiration.getOrElse(null))
            Option(JobOutput(job.location, job.file_size, Option(created), dfe, dle, de))
        } else Option(JobOutput())

        val djp = getDateInMillis(job.dt_job_processing.getOrElse(null))
        val djc = getDateInMillis(job.dt_job_completed.getOrElse(null))
        val stats = if (processed) {
            Option(JobStats(job.dt_job_submitted.get.getMillis, djp, djc, Option(job.input_events.getOrElse(0)), Option(job.output_events.getOrElse(0)), Option(job.latency.getOrElse(0)), Option(job.execution_time.getOrElse(0L))))
        } else Option(JobStats(job.dt_job_submitted.get.getMillis))
        val request = JSONUtils.deserialize[Request](job.request_data.getOrElse("{}"))
        val lastupdated = djc.getOrElse(djp.getOrElse(job.dt_job_submitted.get.getMillis))
        JobResponse(job.request_id.get, job.status.get, lastupdated, request, job.iteration.getOrElse(0), output, stats)
    }

    private def _saveJobRequest(requestId: String, clientKey: String, request: Request, iteration: Int = 0): JobRequest = {
        val status = JobStatus.SUBMITTED.toString()
        val jobSubmitted = DateTime.now()
        val jobRequest = JobRequest(Option(clientKey), Option(requestId), None, Option(status), Option(JSONUtils.serialize(request)), Option(iteration), Option(jobSubmitted), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
        DBUtil.saveJobRequest(Array(jobRequest))
        jobRequest
    }

    private def _getRequestId(filter: Filter, outputFormat: String, datasetId: String, clientKey: String): String = {
        Sorting.quickSort(filter.tags.getOrElse(Array()))
        Sorting.quickSort(filter.events.getOrElse(Array()))
        val key = Array(filter.start_date.get, filter.end_date.get, filter.tags.getOrElse(Array()).mkString, filter.events.getOrElse(Array()).mkString, filter.app_id.getOrElse(""), filter.channel.getOrElse(""), outputFormat, datasetId, clientKey).mkString("|")
        MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString
    }
    private def _validateRequest(channel: String, eventType: String, from: String, to: String)(implicit config: Config): Map[String, String] = {

        APILogger.log("Validating Request", Option(Map("channel" -> channel, "eventType" -> eventType, "from" -> from, "to" -> to)))
        if (!EVENT_TYPES.contains(eventType)) {
            return Map("status" -> "false", "message" -> "Please provide 'eventType' value should be one of these -> ('raw' or 'summary' or 'metrics', or 'failed') in your request URL")
        }
        if (StringUtils.isBlank(from)) {
            return Map("status" -> "false", "message" -> "Please provide 'from' in query string")
        }
        val days = CommonUtil.getDaysBetween(from, to)
        if (CommonUtil.getPeriod(to) > CommonUtil.getPeriod(CommonUtil.getToday))
            return Map("status" -> "false", "message" -> "'to' should be LESSER OR EQUAL TO today's date..")
        else if (0 > days)
            return Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your 'from' & 'to'")
        else if (10 < days)
            return Map("status" -> "false", "message" -> "Date range should be < 10 days")
        else return Map("status" -> "true")
    }
}

class JobAPIService extends Actor {
    import JobAPIService._

    def receive = {
        case DataRequest(request: String, channelId: String, config: Config) => sender() ! dataRequest(request, channelId)(config)
        case GetDataRequest(clientKey: String, requestId: String, config: Config) => sender() ! getDataRequest(clientKey, requestId)(config)
        case DataRequestList(clientKey: String, limit: Int, config: Config) => sender() ! getDataRequestList(clientKey, limit)(config)
        case ChannelData(channel: String, eventType: String, from: String, to: String, config: Config, code: Option[String]) => sender() ! getChannelData(channel, eventType, from, to, code)(config)
    }

}