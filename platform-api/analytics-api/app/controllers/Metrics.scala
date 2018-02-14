package controllers

import akka.actor.ActorSystem
import akka.pattern._
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import scala.concurrent.Future

import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.service.MetricsAPIService
import org.ekstep.analytics.api.service.MetricsAPIService._
import org.ekstep.analytics.api.MetricsRequestBody
import akka.actor.Props
import akka.routing.FromConfig

/**
 * @author mahesh
 */

@Singleton
class Metrics @Inject() (system: ActorSystem) extends BaseController {
    implicit val className = "controllers.Metrics";
    val metricsAPIActor = system.actorOf(Props[MetricsAPIService].withRouter(FromConfig()), name = "metricsApiActor");

    def get(datasetId: String, summary: String) = Action.async { implicit request =>

        val bodyStr: String = Json.stringify(request.body.asJson.get);
        val body = JSONUtils.deserialize[MetricsRequestBody](bodyStr);
        val result = ask(metricsAPIActor, Metrics(datasetId, summary, body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def contentUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val result = ask(metricsAPIActor, ContentUsage(body, config)).mapTo[String];

        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def contentPopularity() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val fields = request.getQueryString("fields").getOrElse("NA").split(",");
        val result = ask(metricsAPIActor, ContentPopularity(body, fields, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def itemUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val result = ask(metricsAPIActor, ItemUsage(body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def genieLaunch() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val result = ask(metricsAPIActor, GenieLaunch(body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def contentList() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val result = ask(metricsAPIActor, ContentList(body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def workflowUsage() = Action.async { implicit request =>
        val body = _getMetricsRequest(request);
        val result = ask(metricsAPIActor, WorkflowUsage(body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    private def _getMetricsRequest(request: Request[AnyContent]) = {
        val body: String = Json.stringify(request.body.asJson.get);
        JSONUtils.deserialize[MetricsRequestBody](body);
    }
}
