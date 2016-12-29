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
import org.ekstep.analytics.api.JobRequestEvent
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.JobStatus
import org.ekstep.analytics.api.JobStats
import org.ekstep.analytics.api.APIIds
import org.joda.time.DateTime


/**
 * @author mahesh
 */

object JobAPIService {
	
	def props = Props[JobAPIService];
	case class DataRequest(request: String, sc: SparkContext, config: Config);
	case class GetDataRequest(clientKey: String, requestId: String, sc: SparkContext, config: Config);
	case class DataRequestList(clientKey: String, limit: Int, sc: SparkContext, config: Config);
	
	def dataRequest(request: String)(implicit sc: SparkContext, config: Config): String = {
		val body = JSONUtils.deserialize[RequestBody](request);
		val isValid = _validateReq(body)
		if ("true".equals(isValid.get("status").get)) {
			val requestId = _getRequestId(body.request.filter.get);
			val job = DBUtil.getJobRequest(requestId, body.params.get.client_key.get);
			val jobResponse = if (null == job) {
				_saveJobRequest(requestId, body);
			} else {
                _createJobResponse(job)
            }
			val response = CommonUtil.ccToMap(jobResponse)
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
            JSONUtils.serialize(CommonUtil.OK(APIIds.GET_DATA_REQUEST, CommonUtil.ccToMap(jobStatusRes)));
		}
	}
	
	def getDataRequestList(clientKey: String, limit: Int)(implicit sc: SparkContext, config: Config): String = {
		val currDate = DateTime.now();
		val rdd = DBUtil.getJobRequestList(clientKey);
		val jobs = rdd.filter { f => f.dt_expiration.getOrElse(currDate).getMillis >= currDate.getMillis }
		val result = jobs.take(limit).map { x => _createJobResponse(x) }
		JSONUtils.serialize(CommonUtil.OK(APIIds.GET_DATA_REQUEST_LIST, Map("count" -> Long.box(jobs.count()), "jobs" -> result)));	
	}
	
	private def _validateReq(body: RequestBody): Map[String, String] = {
		val params = body.params
		val filter = body.request.filter;
		if (filter.isEmpty || params.isEmpty) {
			val message = if (filter.isEmpty) "filter is empty" else "filter is empty";
			Map("status" -> "false", "message" -> message);
		} else {
			if (filter.get.start_date.isEmpty || filter.get.end_date.isEmpty || params.get.client_key.isEmpty || filter.get.tags.isEmpty) {
				val message = if (params.get.client_key.isEmpty) "client_key is empty" 
					else if(filter.get.tags.isEmpty)
						"tags are empty"
					else "start date or end date is empty"
				Map("status" -> "false", "message" -> message);
			} else {
				val endDate = filter.get.end_date.get
				val startDate = filter.get.start_date.get
				val days = CommonUtil.getDaysBetween(startDate, endDate)
				if (CommonUtil.getPeriod(endDate) >= CommonUtil.getPeriod(CommonUtil.getToday)) 
					Map("status" -> "false", "message" -> "end_date should be lesser than today's date.."); 
				else if (0 > days) 
					Map("status" -> "false", "message" -> "Date range should not be -ve. Please check your start_date & end_date");
				else if(30 < days)
				    Map("status" -> "false", "message" -> "Date range should be < 30 days");
				else Map("status" -> "true");	
			}
		}
    }
	
	private def _createJobResponse(job: JobRequest): JobResponse = {
		val processed = List(JobStatus.COMPLETE.toString(), JobStatus.FAILED.toString()).contains(job.status);
		val created = if (job.dt_file_created.isEmpty) "" else job.dt_file_created.get.getMillis.toString()
        val output = if (processed) Option(JobOutput(job.location, job.file_size, Option(created), Option(job.dt_first_event.get.getMillis), Option(job.dt_last_event.get.getMillis), Option(job.dt_expiration.get.getMillis))) else Option(JobOutput());
        val stats = if (processed) Option(JobStats(job.dt_job_submitted.get.getMillis, Option(job.dt_job_processing.get.getMillis), Option(job.dt_job_completed.get.getMillis), Option(job.input_events.getOrElse(0)), Option(job.output_events.getOrElse(0)), Option(job.latency.getOrElse(0)), Option(job.execution_time.getOrElse(0L)))) else Option(JobStats(job.dt_job_submitted.get.getMillis));
        val request = JSONUtils.deserialize[Request](job.request_data.getOrElse("{}"))
        JobResponse(job.request_id.get, job.status.get, CommonUtil.getMillis, request, output, stats);
    }
	
	private def _saveJobRequest(requestId: String, body: RequestBody)(implicit sc: SparkContext): JobResponse = {
		val status = JobStatus.SUBMITTED.toString();
		val jobSubmitted = DateTime.now()
		val jobRequest = JobRequest(body.params.get.client_key, Option(requestId), None, Option(status), Option(JSONUtils.serialize(body.request)), Option(1), Option(jobSubmitted))
		DBUtil.saveJobRequest(jobRequest);
		JobResponse(requestId, status, jobSubmitted.getMillis, body.request);
	}
	
	private def _getRequestId(filter: Filter): String = {
		Sorting.quickSort(filter.tags.getOrElse(Array()));
		Sorting.quickSort(filter.events.getOrElse(Array()));
		val key = Array(filter.start_date.get, filter.end_date.get, filter.tags.get.mkString, filter.events.getOrElse(Array()).mkString).mkString("|");
		MessageDigest.getInstance("MD5").digest(key.getBytes).map("%02X".format(_)).mkString;
    }
	
	private def _getJobRequestEvent(requestId: String, body: RequestBody): JobRequestEvent = {
        val client_id = body.params.get.client_key.get
        val filter = body.request.filter
        val eksMap = Map("request_id" -> requestId, "job_id" -> "data-exhaust", "filter" -> filter.get)
        JobRequestEvent("BE_JOB_REQUEST", System.currentTimeMillis(), "1.0", Context(PData("AnalyticsAPIPipeline", null, "1.0", Option("")), None, null, null, Option(JobStatus.SUBMITTED.toString), Option(client_id), Option(1)), MEEdata(eksMap))
    }
}

class JobAPIService extends Actor {
	import JobAPIService._;
	
	def receive = {
		case DataRequest(request: String, sc: SparkContext, config: Config) => sender() ! dataRequest(request)(sc, config);
		case GetDataRequest(clientKey: String, requestId: String, sc: SparkContext, config: Config) => sender() ! getDataRequest(clientKey, requestId)(sc, config);
		case DataRequestList(clientKey: String, limit: Int, sc: SparkContext, config: Config) => sender() ! getDataRequestList(clientKey, limit)(sc, config);
	}
}