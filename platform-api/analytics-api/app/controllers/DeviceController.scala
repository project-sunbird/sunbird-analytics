package controllers

import akka.actor._
import appconf.AppConf
import com.google.inject.Inject
import org.ekstep.analytics.api.service.{ExperimentData, ExperimentRequest, RegisterDevice}
import org.ekstep.analytics.api.util.{APILogger, CommonUtil, JSONUtils}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.Action
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class DeviceController @Inject()(system: ActorSystem) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")

  def registerDevice(deviceId: String) = Action.async(parse.json) { implicit request =>
    val deviceRegisterServiceAPIActor: ActorRef = AppConf.getActorRef("deviceRegisterService")
    val experimentActor: ActorRef = AppConf.getActorRef("experimentService")
    var experimentInputMap: Map[String, String] = Map("deviceId" -> deviceId)
    implicit val timeout = Timeout(5 seconds)

    val body: JsValue = request.body
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
    val dspec: Option[String] = (body \ "request" \ "dspec").toOption  match {
      case Some(value) => Option(Json.stringify(value))
      case None => None
    }

    (body \ "request" \ "ext").toOption match {
      case Some(value) => {
        val userId = (value \ "userid").asOpt[String]
        val url = (value \ "url").asOpt[String]
        userId match {
          case Some(data) => experimentInputMap += ("userId" -> data)
          case None => None
        }

        url match {
          case Some(data) => experimentInputMap += ("url" -> data)
          case None => None
        }
      }
      case None => None
    }

    producer match {
      case Some(data) => experimentInputMap += ("platform" -> data)
      case None => None
    }

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, headerIP, ipAddr, fcmToken, producer, dspec, uaspec), ActorRef.noSender)

    val result = experimentActor ? ExperimentRequest(experimentInputMap)
    result.map { expData => {
      var logData: Map[String, String] = experimentInputMap
        val res = expData match {
          case Some(data: ExperimentData) => {
            logData = logData ++ Map("title" -> "experiment", "experimentId" -> data.id, "experimentName" -> data.name, "key" -> data.key)
            List(Map("type" -> "experiment", "data" -> data))
          }
          case None => List()
        }

        APILogger.log("", Option(Map("type" -> "api_access",
        "params" -> List(logData ++ Map("status" -> 200, "method" -> "POST",
          "rid" -> "experimentService", "title" -> "experimentService")))),
        "experimentService")

        Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
          Map("message" -> s"Device registered successfully", "actions" -> res))))
          .withHeaders(CONTENT_TYPE -> "application/json")
      }
    }.recover {
      case ex: Exception => {
        APILogger.log("", Option(Map("type" -> "api_access",
          "params" -> List(Map("status" -> 500, "method" -> "POST",
            "rid" -> "experimentService", "title" -> "experimentService")), "data" -> ex.getMessage)),
          "experimentService")
        InternalServerError(
          JSONUtils.serialize(CommonUtil.errorResponse("analytics.device-register",
            ex.getMessage, "ERROR"))
        ).withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }

}
