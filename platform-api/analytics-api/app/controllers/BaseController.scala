package controllers

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import akka.util.Timeout.durationToTimeout
import com.typesafe.config.Config
import org.ekstep.analytics.api.service.CacheRefreshActor
import play.api.Configuration
import play.api.mvc._

import scala.concurrent.duration.DurationInt

/**
 * @author mahesh
 */

class BaseController(cc: ControllerComponents, configuration: Configuration) extends AbstractController(cc) {

  implicit val className = "controllers.BaseController"

  implicit lazy val config: Config = configuration.underlying

  implicit val timeout: Timeout = 20 seconds

    def result(code: String, res: String): Result = {
        val resultObj = code match {
            case "OK" =>
                Ok(res)
            case "CLIENT_ERROR" =>
                BadRequest(res)
            case "SERVER_ERROR" =>
                InternalServerError(res)
            case "REQUEST_TIMEOUT" =>
                RequestTimeout(res)
            case "RESOURCE_NOT_FOUND" =>
                NotFound(res)
            case "FORBIDDEN" =>
                Forbidden(res)
        }
        resultObj.withHeaders(CONTENT_TYPE -> "application/json")
    }
}