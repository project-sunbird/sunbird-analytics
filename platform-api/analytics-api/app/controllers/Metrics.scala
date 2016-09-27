package controllers

import org.ekstep.analytics.api.MetricsRequestBody
import org.ekstep.analytics.api.ResponseCode
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.service.MockMetricsAPIService
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils

import akka.actor.ActorSystem
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.json.Json
import play.api.mvc.Action
import play.api.mvc.AnyContent
import play.api.mvc.Request

/**
 * @author mahesh
 */

@Singleton
class Metrics @Inject() (system: ActorSystem) extends BaseController {
	implicit val className = "controllers.Metrics";

	def contentUsage() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MockMetricsAPIService.contentUsage(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.content-usage", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def contentPopularity() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MockMetricsAPIService.contentPopularity(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.content-popularity", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def itemUsage() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MockMetricsAPIService.itemUsage(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.item-usage", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def genieLaunch() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MockMetricsAPIService.genieLaunch(body)(Context.sc);
			Ok(result).withHeaders(CONTENT_TYPE -> "application/json");
		} catch {
			case ex: ClientException =>
				Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.metrics.genie-launch", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
		}
	}

	def contentList() = Action { implicit request =>
		try {
			val body = _getMetricsRequest(request);
			val result = MockMetricsAPIService.contentList(body)(Context.sc);
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
