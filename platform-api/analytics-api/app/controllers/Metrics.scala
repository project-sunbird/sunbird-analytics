package controllers

import javax.inject.Singleton
import javax.inject.Inject
import akka.actor.ActorSystem
import play.api.mvc._
import play.api.libs.json._
import akka.util.Timeout
import scala.concurrent.duration._
import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.api.service.MetricsAPIService
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.api.service.MetricsAPIService
import org.ekstep.analytics.api.service.MetricsAPIService
import context.Context
import play.api.mvc.Request
import org.apache.commons.lang.StringUtils
import org.ekstep.analytics.api.exception.ClientException


@Singleton
class Metrics @Inject() (system: ActorSystem) extends Controller {
	implicit val className = "controllers.Metrics";
	implicit val timeout: Timeout = 20 seconds;
	
	def contentUsage() = Action { implicit request =>
		try {
		val body = _getMetricsRequest(request);
		val result = MetricsAPIService.contentUsage(body)(Context.sc);
		Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.content-usage", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
	
	def contentPopularity() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MetricsAPIService.contentPopularity(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.content-popularity", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
	
	def genieUsage() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MetricsAPIService.genieUsage(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.genie-usage", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
	
	def contentList() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MetricsAPIService.contentList(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.content-list", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}
	
	private def _getMetricsRequest(request: Request[AnyContent]) = {
		val body: String = Json.stringify(request.body.asJson.get);
		JSONUtils.deserialize[MetricsRequestBody](body);
	}
}
