package controllers

import akka.actor._
import akka.pattern.ask
import com.google.inject.Inject
import org.ekstep.analytics.api.service.experiment.{ExperimentData, ExperimentRequest}
import org.ekstep.analytics.api.service.{DeviceProfile, GetDeviceProfile, RegisterDevice}
import org.ekstep.analytics.api.util.{APILogger, CacheUtil, CommonUtil, JSONUtils}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{AnyContent, ControllerComponents, Request, Result}
import play.api.{Configuration, Environment}
import javax.inject._

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(
  @Named("device-register-actor") deviceRegisterActor: ActorRef,
  @Named("experiment-actor") experimentActor: ActorRef,
  system: ActorSystem,
  configuration: Configuration,
  cc: ControllerComponents,
  env: Environment
) extends BaseController(cc, configuration) {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")
  lazy val isExperimentEnabled: Boolean = configuration.getBoolean("deviceRegisterAPI.experiment.enable").getOrElse(false)

  def registerDevice(deviceId: String) = Action.async { request: Request[AnyContent] =>
    val body: JsValue = request.body.asJson.get
    // The X-Forwarded-For header from Azure is in the format '61.12.65.222:33740, 61.12.65.222'
    val ip = request.headers.get("X-Forwarded-For").map {
      x =>
        val ipArray = x.split(",")
        if (ipArray.length == 2) ipArray(1).trim else ipArray(0).trim
    }

    val headerIP = ip.getOrElse("")
    val uaspec = request.headers.get("User-Agent")
    val ipAddr = (body \ "request" \ "ip_addr").asOpt[String]
    val fcmToken = (body \ "request" \ "fcmToken").asOpt[String]
    val producer = (body \ "request" \ "producer").asOpt[String]
    val firstAccess = (body \ "request" \ "first_access").asOpt[Long]
    val dspec: Option[String] = (body \ "request" \ "dspec").toOption.map {
      value => Json.stringify(value)
    }
    val userDefinedState = (body \ "request" \ "userDeclaredLocation" \ "state").asOpt[String]
    val userDefinedDistrict = (body \ "request" \ "userDeclaredLocation" \ "district").asOpt[String]

    val extMap: Option[Map[String, String]] = (body \ "request" \ "ext").toOption.map {
      value => {
        val userId = (value \ "userid").asOpt[String]
        val url = (value \ "url").asOpt[String]
        val userIdMap = userId.map { data =>  Map("userId" -> data) }
        val urlMap = url.map { data => Map("url" -> data) }
        userIdMap.getOrElse(Map()) ++ urlMap.getOrElse(Map())
      }
    }

    deviceRegisterActor.tell(RegisterDevice(deviceId, headerIP, ipAddr, fcmToken, producer, dspec, uaspec, firstAccess, userDefinedState, userDefinedDistrict), ActorRef.noSender)

    if (isExperimentEnabled) {
      sendExperimentData(Some(deviceId), extMap.getOrElse(Map()).get("userId"), extMap.getOrElse(Map()).get("url"), producer)
    } else {
      Future {
        Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
          Map("message" -> s"Device registered successfully", "actions" -> List()))))
          .withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }

  def sendExperimentData(deviceId: Option[String], userId: Option[String], url: Option[String], producer: Option[String]): Future[Result] = {
    val result = (experimentActor ? ExperimentRequest(deviceId, userId, url, producer)).mapTo[Option[ExperimentData]]

    result.map {
      expData => {
      var log: Map[String, String] = Map("experimentAssigned" -> "false", "userId" -> userId.orNull,
        "deviceId" -> deviceId.orNull, "url" -> url.orNull, "producer" -> producer.orNull)

      val res = expData match {
        case Some(data: ExperimentData) => {
          val result = Map("title" -> "experiment", "experimentId" -> data.id, "experimentName" -> data.name,
            "key" -> data.key, "startDate" -> data.startDate, "endDate" -> data.endDate)
          log = log ++ result ++ Map("experimentAssigned" -> "true")
          List(Map("type" -> "experiment", "data" -> result))
        }
        case None => List()
      }

      APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(log ++ Map("status" -> 200, "method" -> "POST",
          "rid" -> "experimentService", "title" -> "experimentService")))), "ExperimentService")

      Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully", "actions" -> res)))).withHeaders(CONTENT_TYPE -> "application/json")
      }

    }.recover {
      case ex: Exception => {
        APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("status" -> 500, "method" -> "POST",
            "rid" -> "experimentService", "title" -> "experimentService")), "data" -> ex.getMessage)), "ExperimentService")

        InternalServerError(
          JSONUtils.serialize(CommonUtil.errorResponse("analytics.device-register", ex.getMessage, "ERROR"))
        ).withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }

  def getDeviceProfile(deviceId: String) = Action.async { implicit request =>
    // The X-Forwarded-For header from Azure is in the format '61.12.65.222:33740, 61.12.65.222'
    val ip = request.headers.get("X-Forwarded-For").map {
      x =>
        val ipArray = x.split(",")
        if (ipArray.length == 2) ipArray(1).trim else ipArray(0).trim
    }
    val headerIP = ip.getOrElse("")
    println(s"calling deviceRegisterServiceAPIActor with $headerIP, $deviceId, ${deviceRegisterActor.path}")
    val result = (deviceRegisterActor ? GetDeviceProfile(deviceId, headerIP)).mapTo[Option[DeviceProfile]]
    result.map {
      deviceData =>
        if (deviceData.nonEmpty)
        {
          APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("userDeclaredLocation" -> deviceData.get.userDeclaredLocation, "ipLocation" -> deviceData.get.ipLocation) ++ Map("status" -> 200, "method" -> "POST",
            "rid" -> "getDeviceProfile", "title" -> "getDeviceProfile")))), "getDeviceProfile")

          Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-profile",
            Map("userDeclaredLocation" -> deviceData.get.userDeclaredLocation, "ipLocation" -> deviceData.get.ipLocation))))
            .withHeaders(CONTENT_TYPE -> "application/json")
        }
        else {
          InternalServerError(
            JSONUtils.serialize(CommonUtil.errorResponse("analytics.device-profile", "IP is missing in the header", "ERROR"))
          ).withHeaders(CONTENT_TYPE -> "application/json")
        }

    }.recover {
      case ex: Exception => {
        ex.printStackTrace()
        APILogger.log("", Option(Map("type" -> "api_access", "params" -> List(Map("status" -> 500, "method" -> "POST",
          "rid" -> "getDeviceProfile", "title" -> "getDeviceProfile")), "data" -> ex.getMessage)), "getDeviceProfile")

        InternalServerError(
          JSONUtils.serialize(CommonUtil.errorResponse("analytics.device-profile", ex.getMessage, "ERROR"))
        ).withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }
}
