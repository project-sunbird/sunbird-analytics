package controllers

import akka.actor.ActorSystem
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request

import com.datastax.spark.connector._

import org.apache.spark.SparkContext

import org.joda.time.DateTime

import org.ekstep.analytics.api.JobSummary
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.service.JobAPIService
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.api.JobStatus

import org.ekstep.analytics.api.RequestBody
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.api.JobStatusResponse
import org.ekstep.analytics.api.JobRequestEvent
import org.ekstep.analytics.api.JobOutput
import org.ekstep.analytics.api.JobStats
import org.ekstep.analytics.framework._

/**
 * @author mahesh
 */

@Singleton
class JobController @Inject() (system: ActorSystem) extends BaseController {
    implicit val className = "controllers.JobController";
    val jobAPIActor = system.actorOf(JobAPIService.props, "job-api-service-actor");

    private def _getJobRequest(request: Request[AnyContent]) = {
        val body: String = Json.stringify(request.body.asJson.get);
        JSONUtils.deserialize[RequestBody](body);
    }

    private def _getRequestId(filter: Filter): String = {
        Integer.toString(Math.abs(Array(filter.start_date.get, filter.end_date.get, filter.tags.get.mkString, filter.events.getOrElse(Array()).mkString).mkString.hashCode))
    }

    private def _validateReq(filter: Filter): String = {
        val endDate = filter.end_date.get
        val startDate = filter.start_date.get
        if (CommonUtil.getPeriod(endDate) >= CommonUtil.getPeriod(CommonUtil.getToday)) "<end_date> should be lesser than today's date.."; else if (30 < CommonUtil.getDaysBetween(startDate, endDate)) "'Date range' < 30 days" else "";
    }

    private def _getJobRequestEvent(requestId: String, body: RequestBody): JobRequestEvent = {
        val client_id = body.param.get.client_id.get
        val filter = body.request.filter
        val endDate = filter.get.end_date.get
        val startDate = filter.get.start_date.get

        val jobConfig = JobConfig(Fetcher("S3", None, Option(Array(Query(Option(config.getString("data_exhaust.s3_bucket")), Option("raw/"), Option(startDate), Option(endDate), Option(0), None, None, None, None, None)))), None, null, "org.ekstep.analytics.model.DataExhaustModel", None, Option(Array(Dispatcher("kafka", Map("brokerList" -> config.getString("dataExhaust.data.brokerList"), "topic" -> config.getString("dataExhaust.data.topic"))))), Option(10), Option("Data Exhaust"), None)
        val eksMap = Map("request_id" -> requestId, "job_id" -> "data-exhaust", "config" -> jobConfig)
        JobRequestEvent("BE_JOB_REQUEST", System.currentTimeMillis(), "1.0", Context(PData("AnalyticsAPIPipeline", null, "1.0", Option("")), None, null, null, Option(JobStatus.SUBMITTED.toString), Option(client_id), Option(1)), MEEdata(eksMap))
    }

    def dataExhaust() = Action { implicit request =>
        try {

            val body = _getJobRequest(request);
            val filter = body.request.filter.get
            val msg = _validateReq(filter)
            if (!"".equals(msg)) {
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data-exhaust", Map("message" -> msg, "request" -> body.request)));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            }
            val requestId = _getRequestId(filter)
            val job = JobAPIService.checkTheJob(requestId, body.param.get.client_id.get)(context.Context.sc)
            if (null == job) {
                val event = _getJobRequestEvent(requestId, body)
                val brokerList = config.getString("dataExhaust.job.brokerList")
                val topic = config.getString("dataExhaust.job.topic")
                (jobAPIActor ! JobAPIService.dataExhaust(event, body, brokerList, topic)(context.Context.sc, config))
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data-exhaust", Map("message" -> msg, "job_id" -> requestId, "status" -> "SUBMITTED", "last_updated" -> Long.box(System.currentTimeMillis), "request" -> body.request)));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            } else {
                val jobStatusRes = JobAPIService.getJobStatusResponse(job)
                val result = JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(jobStatusRes))
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data.out", result));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            }

        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.data.out", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJob(clientId: String, jobId: String) = Action { implicit request =>
        try {
            val result = JobAPIService.getJob(clientId, jobId)(context.Context.sc)
            Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.job.info", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJobList(clientId: String) = Action { implicit request =>
        try {
            val result = JobAPIService.getJobList(clientId)(context.Context.sc)
            Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.job.list", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
}