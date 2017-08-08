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

import scala.concurrent.Future

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
import org.ekstep.analytics.api.AggregateMetricsRequestBody
import org.ekstep.analytics.api.service.MetricsAPIService.AggregateMetrics
import akka.actor.Props
import akka.routing.FromConfig

/**
 * @author mahesh
 */

@Singleton
class Metrics @Inject() (system: ActorSystem) extends BaseController {
    implicit val className = "controllers.Metrics";
    val metricsAPIActor = system.actorOf(Props[MetricsAPIService].withRouter(FromConfig()), name = "metricsApiActor");

    // dummy API
    def usageMetrics(datasetId: String, summary: String) = Action.async { implicit request =>
        // TODO: Add code for all metrics of both consumption & creation  
        val response = """{"id":"ekstep.analytics.metrics.content-usage","ver":"1.0","ts":"2017-07-27T09:35:15.027+00:00","params":{"resmsgid":"56de001d-b8e2-4a9e-b631-9a1b59b12e71","status":"successful"},"responseCode":"OK","result":{"metrics":[{"d_period":20170726,"label":"Jul 26 Wed","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170725,"label":"Jul 25 Tue","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170724,"label":"Jul 24 Mon","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170723,"label":"Jul 23 Sun","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170722,"label":"Jul 22 Sat","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170721,"label":"Jul 21 Fri","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0},{"d_period":20170720,"label":"Jul 20 Thu","m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0}],"summary":{"m_total_ts":0,"m_total_sessions":0,"m_avg_ts_session":0,"m_total_interactions":0,"m_avg_interactions_min":0,"m_total_devices":0,"m_avg_sess_device":0}}}"""
        val result: Future[String] = Future { response }
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def aggregateMetrics(datasetId: String, summary: String) = Action.async { implicit request =>

        val bodyStr: String = Json.stringify(request.body.asJson.get);
        val body = JSONUtils.deserialize[AggregateMetricsRequestBody](bodyStr);
        println("bodyStr: "+ bodyStr)
        val result = ask(metricsAPIActor, AggregateMetrics(datasetId, summary, body, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

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
