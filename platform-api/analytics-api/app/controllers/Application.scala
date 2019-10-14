package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.routing.FromConfig
import javax.inject.{Inject, Singleton}
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.service.HealthCheckAPIService.GetHealthStatus
import org.ekstep.analytics.api.service.RecommendationAPIService.{Consumption, Creation}
import org.ekstep.analytics.api.service.TagService.{DeleteTag, RegisterTag}
import org.ekstep.analytics.api.service.{DruidHealthCheckService, _}
import org.ekstep.analytics.api.util._
import org.ekstep.analytics.api.{APIIds, ResponseCode}
import org.ekstep.analytics.framework.util.RestUtil
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * @author mahesh
 */

@Singleton
class Application @Inject() (system: ActorSystem) extends BaseController {

	implicit override val className = "controllers.Application"
	val recommendAPIActor = system.actorOf(Props[RecommendationAPIService].withRouter(FromConfig()), name = "recommendAPIActor")
	val healthCheckAPIActor = system.actorOf(Props[HealthCheckAPIService].withRouter(FromConfig()), name = "healthCheckAPIActor")
	val tagServiceAPIActor = system.actorOf(Props[TagService].withRouter(FromConfig()), name = "tagServiceAPIActor")
	val clientLogAPIActor = system.actorOf(Props[ClientLogsAPIService].withRouter(FromConfig()), name = "clientLogAPIActor")
	val druidHealthActor = system.actorOf(Props(new DruidHealthCheckService(RestUtil)), "druidHealthActor")
	/*val deviceRegisterServiceAPIActor = system.actorOf(Props[DeviceRegisterService].withRouter(FromConfig()),
		name = "deviceRegisterServiceAPIActor")*/

	def getDruidHealthStatus() = Action.async { implicit request =>
		val result = ask(druidHealthActor, "health").mapTo[String]
		result.map { x =>
			Ok(x).withHeaders(CONTENT_TYPE -> "text/plain");
		}
	}

	def checkAPIhealth() = Action.async { implicit request =>
    val result = ask(healthCheckAPIActor, GetHealthStatus).mapTo[String]
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
	}

	def recommendations() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get);
		val futureRes = ask(recommendAPIActor, Consumption(body, config)).mapTo[String]
		val timeoutFuture = play.api.libs.concurrent.Promise.timeout(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", "request timeout", ResponseCode.REQUEST_TIMEOUT.toString()), 3.seconds);
		val firstCompleted = Future.firstCompletedOf(Seq(futureRes, timeoutFuture))
		val response: Future[String] = firstCompleted.recoverWith {
			case ex: ClientException =>
				Future { CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString()) }
		};
		response.map { resp =>
			play.Logger.info(request + " body - " + body + "\n\t => " + resp)
			val result = if (resp.contains(ResponseCode.CLIENT_ERROR.toString) || config.getBoolean("recommendation.enable")) resp
			else JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations", Map[String, AnyRef]("content" -> List(), "count" -> Int.box(0))))
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json")
		}
	}
	
	def creationRecommendations() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get)
		val futureRes = ask(recommendAPIActor, Creation(body, config)).mapTo[String]
		val timeoutFuture = play.api.libs.concurrent.Promise.timeout(CommonUtil.errorResponseSerialized("ekstep.analytics.creation.recommendations", "request timeout", ResponseCode.REQUEST_TIMEOUT.toString()), 3.seconds);
		val firstCompleted = Future.firstCompletedOf(Seq(futureRes, timeoutFuture))
		val response: Future[String] = firstCompleted.recoverWith {
			case ex: ClientException =>
				Future { CommonUtil.errorResponseSerialized("ekstep.analytics.creation.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString()) };
		}
		response.map { result =>
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json")
		}
	}
	
	def registerTag(tagId: String) = Action.async { implicit request =>
		val result = ask(tagServiceAPIActor, RegisterTag(tagId)).mapTo[String]
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
    }
	}

	def deleteTag(tagId: String) = Action.async { implicit request =>
		val result = ask(tagServiceAPIActor, DeleteTag(tagId)).mapTo[String]
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
    }
	}

	def logClientErrors() = Action.async { implicit request =>
		val body: String = Json.stringify(request.body.asJson.get)
		try {
			val requestObj = JSONUtils.deserialize[ClientLogRequest](body)
			val validator: ValidatorMessage = validateLogClientErrorRequest(requestObj)
			if (validator.status) {
				clientLogAPIActor.tell(ClientLogRequest(requestObj.request), ActorRef.noSender)
				Future {
					Ok(JSONUtils.serialize(CommonUtil.OK(APIIds.CLIENT_LOG,
						Map("message" -> s"Log captured successfully!"))))
						.withHeaders(CONTENT_TYPE -> "application/json")
				}
			} else {
				Future {
					BadRequest(JSONUtils.serialize(CommonUtil.errorResponse(APIIds.CLIENT_LOG, validator.msg, ResponseCode.CLIENT_ERROR.toString)))
						.withHeaders(CONTENT_TYPE -> "application/json")
				}
			}
		} catch {
			case ex: Exception => Future {
				InternalServerError(JSONUtils.serialize(CommonUtil.errorResponse(APIIds.CLIENT_LOG, ex.getMessage, ResponseCode.SERVER_ERROR.toString )))
					.withHeaders(CONTENT_TYPE -> "application/json")
			}
		}
	}

	def validateLogClientErrorRequest(requestObj: ClientLogRequest): ValidatorMessage = {
			if (!requestObj.validate.status) {
				ValidatorMessage(false, requestObj.validate.msg)
			} else {
				ValidatorMessage(true, "")
			}
	}

	/*
  def registerDevice(deviceId: String) = Action.async { implicit request =>
    val body: String = Json.stringify(request.body.asJson.get)
    // The X-Forwarded-For header from Azure is in the format '61.12.65.222:33740, 61.12.65.222'
    val ipAddr = request.headers.get("X-Forwarded-For").map {
      x =>
        val ipArray = x.split(",")
        if (ipArray.length == 2) ipArray(1).trim else ipArray(0).trim
    }
    val ip = ipAddr.getOrElse("")
    val uaspec = request.headers.get("User-Agent")

		deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, ip, body, uaspec), ActorRef.noSender)
    Future {
      Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully"))))
        .withHeaders(CONTENT_TYPE -> "application/json")
    }
  }
  */
}