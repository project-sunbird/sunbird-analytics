package controllers

import akka.actor.ActorSystem
import akka.pattern._
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.service.JobAPIService
import org.ekstep.analytics.api.service.JobAPIService.DataRequest
import org.ekstep.analytics.api.service.JobAPIService.GetDataRequest
import org.ekstep.analytics.api.service.JobAPIService.DataRequestList

/**
 * @author mahesh
 */

@Singleton
class JobController @Inject() (system: ActorSystem) extends BaseController {
	implicit val className = "controllers.JobController";
	val jobAPIActor = system.actorOf(JobAPIService.props, "jobApiActor");

	def dataRequest() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		val result = ask(jobAPIActor, DataRequest(body, Context.sc, config)).mapTo[String];
		result.map { x =>
			Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def getJob(clientKey: String, requestId: String) = Action.async { implicit request =>
		val result = ask(jobAPIActor, GetDataRequest(clientKey, requestId, Context.sc, config)).mapTo[String];
		result.map { x =>
			Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def getJobList(clientKey: String) = Action.async { implicit request =>
		val limit = Integer.parseInt(request.getQueryString("limit").getOrElse(config.getString("data_exhaust.jobs.list.limit")))
		val result = ask(jobAPIActor, DataRequestList(clientKey, limit, Context.sc, config)).mapTo[String];
		result.map { x =>
			Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
}