package controllers

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.routing.FromConfig
import javax.inject.Inject
import org.ekstep.analytics.api.service.JobAPIService
import org.ekstep.analytics.api.service.JobAPIService._
import org.ekstep.analytics.api.util.{APILogger, CacheUtil, CommonUtil, JSONUtils}
import org.ekstep.analytics.api.{APIIds, ResponseCode, _}
import play.api.Configuration
import play.api.libs.json.Json
import play.api.mvc.{Request, Result, _}

import scala.concurrent.{ExecutionContext, Future}

/**
  * @author Amit Behera, mahesh
  */

class JobController @Inject() (
                                system: ActorSystem,
                                configuration: Configuration,
                                cc: ControllerComponents,
                                cacheUtil: CacheUtil
                              )(implicit ec: ExecutionContext) extends BaseController(cc, configuration) {

  val jobAPIActor = system.actorOf(Props[JobAPIService].withRouter(FromConfig()), name = "jobApiActor")

  def dataRequest() = Action.async { request: Request[AnyContent] =>
    val body: String = Json.stringify(request.body.asJson.get)
    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(consumerId, channelId) else true
    println(s"is authenticated! $checkFlag")
    if (checkFlag) {
      val res = ask(jobAPIActor, DataRequest(body, channelId, config)).mapTo[Response]
      res.map { x =>
        result(x.responseCode, JSONUtils.serialize(x))
      }
    } else {
      val msg = s"Given X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId' are not authorized"
      APILogger.log(s"Authorization FAILED for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
      unauthorized(msg)
    }
  }

  def getJob(clientKey: String, requestId: String) = Action.async { request: Request[AnyContent] =>

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

  def getJobList(clientKey: String) = Action.async { request: Request[AnyContent] =>

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

  def getTelemetry(datasetId: String) = Action.async { request: Request[AnyContent] =>

    val summaryType =  request.getQueryString("type")
    val from = request.getQueryString("from").getOrElse("")
    val to = request.getQueryString("to").getOrElse(org.ekstep.analytics.api.util.CommonUtil.getToday())

    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val checkFlag = if (config.getBoolean("dataexhaust.authorization_check")) authorizeDataExhaustRequest(consumerId, channelId) else true
    if (checkFlag) {
      APILogger.log(s"Authorization Successfull for X-Consumer-ID='$consumerId' and X-Channel-ID='$channelId'")
      val res = ask(jobAPIActor, ChannelData(channelId, datasetId, from, to, config, summaryType)).mapTo[Response]
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
            cacheUtil.initConsumerChannelCache()
          case "DeviceLocation" =>
            cacheUtil.initDeviceLocationCache()
          case _ =>
            cacheUtil.initCache()
      }
      result("OK", JSONUtils.serialize(CommonUtil.OK(APIIds.CHANNEL_TELEMETRY_EXHAUST, Map("msg" -> s"$cacheType cache refreshed successfully"))))
  }

  def authorizeDataExhaustRequest(consumerId: String, channelId: String): Boolean = {
    APILogger.log(s"Authorizing $consumerId and $channelId")
    val status = Option(cacheUtil.getConsumerChannlTable().get(consumerId, channelId))
    if (status.getOrElse(0) == 1) true else false
  }

  def authorizeDataExhaustRequest(request: Request[AnyContent] ): Boolean = {
    val authorizationCheck = config.getBoolean("dataexhaust.authorization_check")
    if(!authorizationCheck) return true

    val consumerId = request.headers.get("X-Consumer-ID").getOrElse("")
    val channelId = request.headers.get("X-Channel-ID").getOrElse("")
    authorizeDataExhaustRequest(consumerId, channelId)
  }
}