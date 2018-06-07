package controllers

import org.ekstep.analytics.api.service.JobAPIService
import org.ekstep.analytics.api.service.JobAPIService._
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.routing.FromConfig
import javax.inject.Inject
import javax.inject.Singleton

import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.{APILogger, CacheUtil, CommonUtil, JSONUtils}
import org.ekstep.analytics.api.{APIIds, ResponseCode}
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.api.mvc._
import play.api.mvc.Result

import scala.concurrent.Future

/**
 * @author Amit Behera, mahesh
 */

@Singleton
class JobController @Inject() (system: ActorSystem) extends BaseController {
    implicit override val className = "controllers.JobController"

    val jobAPIActor = system.actorOf(Props[JobAPIService].withRouter(FromConfig()), name = "jobApiActor")

    def dataRequest() = Action.async { implicit request =>
        val body: String = Json.stringify(request.body.asJson.get)
        val channelId = request.headers.get("X-Channel-ID").getOrElse("")
        val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
        val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(consumerId, channelId) else true
        if (checkFlag) {
            val res = ask(jobAPIActor, DataRequest(body, config)).mapTo[Response]
            res.map { x =>
                result(x.responseCode, JSONUtils.serialize(x))
            }
        } else {
            val msg = s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"
            APILogger.log(s"Authorization FAILED for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
            unauthorized(msg)
        }
    }

    def getJob(clientKey: String, requestId: String) = Action.async { implicit request =>

        if (authorizeDataExhaustRequest(request)) {
            val res = ask(jobAPIActor, GetDataRequest(clientKey, requestId, config)).mapTo[Response]
            res.map { x =>
                result(x.responseCode, JSONUtils.serialize(x))
            }
        } else {
            val msg = "Given X-Consumer-ID and X-Channel-ID are not authorized"
            APILogger.log("Authorization FAILED")
            unauthorized(msg)
        }
    }

    def getJobList(clientKey: String) = Action.async { implicit request =>

        val channelId = request.headers.get("X-Channel-ID").getOrElse("")
        val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
        val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(consumerId, channelId) else true
        if (checkFlag) {
            val limit = Integer.parseInt(request.getQueryString("limit").getOrElse(config.getString("data_exhaust.list.limit")))
            val res = ask(jobAPIActor, DataRequestList(clientKey, limit, config)).mapTo[Response]
            res.map { x =>
                result(x.responseCode, JSONUtils.serialize(x))
            }
        } else {
            val msg = s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"
            APILogger.log(s"Authorization FAILED for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
            unauthorized(msg)
        }
    }

    def getTelemetry(datasetId: String) = Action.async { implicit request =>
        val from = request.getQueryString("from").getOrElse("")
        val to = request.getQueryString("to").getOrElse(org.ekstep.analytics.api.util.CommonUtil.getToday())
        val channelId = request.headers.get("X-Channel-ID").getOrElse("")
        val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
        val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(consumerId, channelId) else true
        if (checkFlag) {
            APILogger.log(s"Authorization Successfull for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
            val res = ask(jobAPIActor, ChannelData(channelId, datasetId, from, to, config)).mapTo[Response]
            res.map { x =>
                result(x.responseCode, JSONUtils.serialize(x))
            }
        } else {
            val msg = s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"
            APILogger.log(s"Authorization FAILED for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
            unauthorized(msg)
        }
    }

    private def unauthorized(msg: String): Future[Result] = {
        val res = CommonUtil.errorResponse(APIIds.CHANNEL_TELEMETRY_EXHAUST, msg, ResponseCode.FORBIDDEN.toString)
        Future {
            result(res.responseCode, JSONUtils.serialize(res))
        }
    }

    def refreshCache(cacheType: String) = Action { implicit request =>
        cacheType match {
            case "ConsumerChannel" =>
                CacheUtil.initConsumerChannelCache()
            case _ =>
                CacheUtil.initCache()
        }
        result("OK", JSONUtils.serialize(CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("msg" -> s"$cacheType cache refresed successfully"))))
    }
}