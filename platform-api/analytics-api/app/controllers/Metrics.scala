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
import org.ekstep.analytics.api.service.MetricsAPIService
import org.ekstep.analytics.api.service.MetricsAPIService.ContentUsage
import org.ekstep.analytics.api.service.MetricsAPIService.ContentPopularity
import org.ekstep.analytics.api.service.MetricsAPIService.ItemUsage
import org.ekstep.analytics.api.service.MetricsAPIService.ContentList
import org.ekstep.analytics.api.service.MetricsAPIService.ContentList
import org.ekstep.analytics.api.service.MetricsAPIService.GenieLaunch
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

  def contentUsage() = Action.async { implicit request =>
    val body = _getMetricsRequest(request);
    val result = ask(metricsAPIActor, ContentUsage(body, Context.sc, config)).mapTo[String];
    
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
  }

  def contentPopularity() = Action.async { implicit request =>
    val body = _getMetricsRequest(request);
    val fields = request.getQueryString("fields").getOrElse("NA").split(",");
    val result = ask(metricsAPIActor, ContentPopularity(body, fields, Context.sc, config)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
  }

  def itemUsage() = Action.async { implicit request =>
    val body = _getMetricsRequest(request);
    val result = ask(metricsAPIActor, ItemUsage(body, Context.sc, config)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
  }

  def genieLaunch() = Action.async { implicit request =>
    val body = _getMetricsRequest(request);
    val result = ask(metricsAPIActor, GenieLaunch(body, Context.sc, config)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
  }

  def contentList() = Action.async { implicit request =>
    val body = _getMetricsRequest(request);
    val result = ask(metricsAPIActor, ContentList(body, Context.sc, config)).mapTo[String];
    result.map { x =>
      Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
    }
  }

  private def _getMetricsRequest(request: Request[AnyContent]) = {
    val body: String = Json.stringify(request.body.asJson.get);
    JSONUtils.deserialize[MetricsRequestBody](body);
  }
}
