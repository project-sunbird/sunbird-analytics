package controllers

import akka.actor.ActorSystem
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request

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
	
	def dataExhaust() = Action { implicit request =>
		try {
			val result = JobAPIService.dataExhaust()(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
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