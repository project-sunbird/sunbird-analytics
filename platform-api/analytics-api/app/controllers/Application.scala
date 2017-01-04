package controllers

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.service.ContentAPIService
import org.ekstep.analytics.api.service.DataProductManagementAPIService
import org.ekstep.analytics.api.service.HealthCheckAPIService
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.api.service.TagService
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.JSONUtils

import akka.actor.ActorSystem
import akka.actor.actorRef2Scala
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.api.mvc.Action

/**
 * @author mahesh
 */

@Singleton
class Application @Inject() (system: ActorSystem) extends BaseController {
	implicit val className = "controllers.Application";
	val contentAPIActor = system.actorOf(ContentAPIService.props, "content-api-service-actor");
	val dpmgmtAPIActor = system.actorOf(DataProductManagementAPIService.props, "dpmgmt-api-service-actor");

	def checkAPIhealth() = Action { implicit request =>
		val response = HealthCheckAPIService.getHealthStatus()(Context.sc)
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def contentToVec(contentId: String) = Action { implicit request =>
		(contentAPIActor ! ContentAPIService.ContentToVec(contentId, Context.sc, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-to-vec", Map("message" -> "Job submitted for content enrichment")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def contentToVecTrainModel() = Action { implicit request =>
		(contentAPIActor ! ContentAPIService.ContentToVecTrainModel(Context.sc, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-to-vec.train.model", Map("message" -> "successful")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def recommendationsTrainModel() = Action { implicit request =>
		(contentAPIActor ! ContentAPIService.RecommendationsTrainModel(Context.sc, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations.train.model", Map("message" -> "successful")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def recommendations() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		val futureRes = Future { RecommendationAPIService.recommendations(body)(Context.sc, config) };
		val timeoutFuture = play.api.libs.concurrent.Promise.timeout(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", "request timeout", ResponseCode.REQUEST_TIMEOUT.toString()), 3.seconds);
		val firstCompleted = Future.firstCompletedOf(Seq(futureRes, timeoutFuture));
		val response: Future[String] = firstCompleted.recoverWith {
			case ex: ClientException =>
				Future { CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString()) };
		};
		response.map { resp =>
			play.Logger.info(request + " body - " + body + "\n\t => " + resp);
			val result = if (resp.contains(ResponseCode.CLIENT_ERROR.toString()) || config.getBoolean("recommendation.enable")) resp
			else JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))));
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def runJob(job: String) = Action { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		(dpmgmtAPIActor ! DataProductManagementAPIService.RunJob(job, body, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.runjob", Map("message" -> "Job submitted")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def replayJob(job: String, from: String, to: String) = Action { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		(dpmgmtAPIActor ! DataProductManagementAPIService.ReplayJob(job, from, to, body, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.replay-job", Map("message" -> "Job submitted")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}
	
	def registerTag(tagId: String) = Action { implicit request =>
		val response = TagService.registerTag(tagId)(Context.sc);
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}

	def deleteTag(tagId: String) = Action { implicit request =>
		val response = TagService.deleteTag(tagId)(Context.sc);
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
	}
}