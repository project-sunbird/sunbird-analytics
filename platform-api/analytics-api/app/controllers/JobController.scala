package controllers

import akka.actor.ActorSystem
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request

import com.datastax.spark.connector._

import org.ekstep.analytics.api.JobSummary
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.service.JobAPIService

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
        Array(filter.start_date.get, filter.end_date.get, filter.tags.get.mkString, filter.events.getOrElse(Array()).mkString).mkString
    }

    private def _validateReq(filter: Filter): String = {
        val endDate = filter.end_date.get
        val startDate = filter.start_date.get
        if (CommonUtil.getPeriod(endDate) >= CommonUtil.getPeriod(CommonUtil.getToday)) "<end_date> should be lesser than today's date.."; else if (30 < CommonUtil.getDaysBetween(startDate, endDate)) "'Date range' < 30 days" else "";
    }

    private def _checkTheJob(requestId: String): JobSummary = {
        val job = sc.cassandraTable[JobSummary]("general_db", "jobs").where("request_id = ?", requestId)
        if (job.isEmpty()) null; else job.first;
    }

    def dataExhaust() = Action { implicit request =>
        try {

            val body = _getJobRequest(request);
            val filter = body.request.filter
            val msg = _validateReq(filter)
            if (!"".equals(msg)) {
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data-exhaust", Map("message" -> msg, "request" -> body.request)));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            }
            val requestId = _getRequestId(filter)

            val job = _checkTheJob(requestId)
            if (null == job) {
                (jobAPIActor ! JobAPIService.dataExhaust(requestId)(Context.sc))
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data-exhaust", Map("message" -> msg, "job_id" -> requestId, "status" -> "SUBMITTED", "last_updated" -> System.currentTimeMillis(), "request" -> body.request)));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            } else {
                val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.data-exhaust", Map("message" -> msg, "job_id" -> requestId, "status" -> "SUBMITTED", "last_updated" -> System.currentTimeMillis(), "request" -> body.request)));
                Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
            }

        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.data.out", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJob(clientId: String, jobId: String) = Action { implicit request =>
        try {
            val result = JobAPIService.getJob(clientId, jobId)(Context.sc)
            Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.job.info", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJobList(clientId: String) = Action { implicit request =>
        try {
            val result = JobAPIService.getJobList(clientId)(Context.sc)
            Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: ClientException =>
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.job.list", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
}