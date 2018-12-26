package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.routing.FromConfig
import javax.inject.{Inject, Singleton}
import org.ekstep.analytics.api.{Filter, MetricsRequestBody}
import org.ekstep.analytics.api.service.MetricsAPIService
import org.ekstep.analytics.api.service.MetricsAPIService._
import org.ekstep.analytics.framework.util.JSONUtils
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent, BodyParsers, Request}

/**
 * @author mahesh
 */

@Singleton
class Metrics @Inject() (system: ActorSystem) extends BaseController {
    implicit override val className: String = "controllers.Metrics"
    val metricsAPIActor: ActorRef = system.actorOf(Props[MetricsAPIService].withRouter(FromConfig()), name = "metricsApiActor")

    def getMetricsSummary(datasetId: String, summary: String) =
        Action.async(BodyParsers.parse.json) { implicit request =>
            val body = JSONUtils.deserialize[MetricsRequestBody](Json.stringify(request.body))
            val result = ask(metricsAPIActor, Metrics(datasetId, summary, body, config)).mapTo[String]
            result.map { res =>
                Ok(res).withHeaders(CONTENT_TYPE -> "application/json")
            }
        }

    def contentUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, ContentUsage(body, config)).mapTo[String]

        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def contentPopularity() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val fields = request.getQueryString("fields").getOrElse("NA").split(",")
        val result = ask(metricsAPIActor, ContentPopularity(body, fields, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def itemUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, ItemUsage(body, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def genieLaunch() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, GenieLaunch(body, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def contentList() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, ContentList(body, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def workflowUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, WorkflowUsage(body, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    def dialcodeUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request)
        val result = ask(metricsAPIActor, DialcodeUsage(body, config)).mapTo[String]
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json")
        }
    }

    private def _getMetricsRequest(request: Request[AnyContent]) = {
        val body: String = Json.stringify(request.body.asJson.get)
        JSONUtils.deserialize[MetricsRequestBody](body)
    }
}
