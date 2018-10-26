package controllers

import org.ekstep.analytics.api.service.HealthCheckAPIService

import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.api.service.TagService
import org.ekstep.analytics.api.util._
import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._
import scala.concurrent.Future
import javax.inject.Singleton
import javax.inject.Inject
import akka.actor.ActorSystem
import akka.pattern._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import akka.util.Timeout
import scala.concurrent.duration._
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import akka.actor.Props
import akka.routing.FromConfig
import org.ekstep.analytics.api.service.HealthCheckAPIService.GetHealthStatus
import org.ekstep.analytics.api.service.TagService.DeleteTag
import org.ekstep.analytics.api.service.TagService.RegisterTag
import org.ekstep.analytics.api.service.RecommendationAPIService.Consumption
import org.ekstep.analytics.api.service.RecommendationAPIService.Creation
import org.ekstep.analytics.api.service.DeviceRegisterService
import org.ekstep.analytics.api.service.DeviceRegisterService.RegisterDevice

/**
 * @author mahesh
 */

@Singleton
class Application @Inject() (system: ActorSystem) extends BaseController {
	implicit override val className = "controllers.Application";
	val recommendAPIActor = system.actorOf(Props[RecommendationAPIService].withRouter(FromConfig()), name = "recommendAPIActor");
	val healthCheckAPIActor = system.actorOf(Props[HealthCheckAPIService].withRouter(FromConfig()), name = "healthCheckAPIActor");
	val tagServiceAPIActor = system.actorOf(Props[TagService].withRouter(FromConfig()), name = "tagServiceAPIActor");
	val deviceRegisterServiceAPIActor = system.actorOf(Props[DeviceRegisterService].withRouter(FromConfig()), name = "deviceRegisterServiceAPIActor");

	def checkAPIhealth() = Action.async { implicit request =>
    val result = ask(healthCheckAPIActor, GetHealthStatus).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
	}

	def recommendations() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		val futureRes = ask(recommendAPIActor, Consumption(body, config)).mapTo[String];
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
	
	def creationRecommendations() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		val futureRes = ask(recommendAPIActor, Creation(body, config)).mapTo[String];
		val timeoutFuture = play.api.libs.concurrent.Promise.timeout(CommonUtil.errorResponseSerialized("ekstep.analytics.creation.recommendations", "request timeout", ResponseCode.REQUEST_TIMEOUT.toString()), 3.seconds);
		val firstCompleted = Future.firstCompletedOf(Seq(futureRes, timeoutFuture));
		val response: Future[String] = firstCompleted.recoverWith {
			case ex: ClientException =>
				Future { CommonUtil.errorResponseSerialized("ekstep.analytics.creation.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString()) };
		};
		response.map { result =>
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
	
	def registerTag(tagId: String) = Action.async { implicit request =>
		val result = ask(tagServiceAPIActor, RegisterTag(tagId)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
	}

	def deleteTag(tagId: String) = Action.async { implicit request =>
		val result = ask(tagServiceAPIActor, DeleteTag(tagId)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
	}
	
	def registerDevice(deviceId: String) = Action.async { implicit request =>
	  val body: String = Json.stringify(request.body.asJson.get);
	  val ip = request.headers.get("X-FORWARDED-FOR").getOrElse("")
		val result = ask(deviceRegisterServiceAPIActor, RegisterDevice(deviceId, ip, body)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
	}
}