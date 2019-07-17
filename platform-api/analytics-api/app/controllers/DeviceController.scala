package controllers

import akka.actor._
import appconf.AppConf
import com.google.inject.Inject
import org.ekstep.analytics.api.service.{ExperimentData, ExperimentRequest, RegisterDevice}
import org.ekstep.analytics.api.util.{APILogger, CommonUtil, JSONUtils}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Result}
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(system: ActorSystem) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")
  lazy val configuration: Config = ConfigFactory.load()
  lazy val isExperimentEnabled = configuration.getBoolean("deviceRegisterAPI.experiment.enable")

  def registerDevice(deviceId: String) = Action.async(parse.json) { implicit request =>
    val deviceRegisterServiceAPIActor: ActorRef = AppConf.getActorRef("deviceRegisterService")
    var inputMap: Map[String, String] = Map("deviceId" -> deviceId)

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
          case Some(data) => inputMap += ("userId" -> data)
          case None => None
        }

        url match {
          case Some(data) => inputMap += ("url" -> data)
          case None => None
        }
      }
      case None => None
    }

    producer match {
      case Some(data) => inputMap += ("platform" -> data)
      case None => None
    }

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, headerIP, ipAddr, fcmToken, producer, dspec, uaspec), ActorRef.noSender)

    if (isExperimentEnabled) {
      sendExperimentData(inputMap)
    } else {
      Future {
        Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
          Map("message" -> s"Device registered successfully", "actions" -> List()))))
          .withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }

  def sendExperimentData(input: Map[String, String]): Future[Result] = {
    val experimentActor: ActorRef = AppConf.getActorRef("experimentService")

    val result = experimentActor ? ExperimentRequest(input)

    result.map { expData => {
      var logData: Map[String, String] = input
      val res = expData match {
        case Some(data: ExperimentData) => {
          val expData = Map("title" -> "experiment", "experimentId" -> data.id, "experimentName" -> data.name, "key" -> data.key, "startDate" -> data.startDate, "endDate" -> data.endDate)
          logData = Map("experimentAssigned" -> "true") ++ logData ++ expData
          List(Map("type" -> "experiment", "data" -> expData))
        }
        case None => List()
      }

      APILogger.log("", Option(Map("type" -> "api_access",
        "params" -> List(logData ++ Map("status" -> 200, "method" -> "POST",
          "rid" -> "experimentService", "title" -> "experimentService")))),
        "ExperimentService")

      Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully", "actions" -> res))))
        .withHeaders(CONTENT_TYPE -> "application/json")
      }
    }.recover {
      case ex: Exception => {
        APILogger.log("", Option(Map("type" -> "api_access",
          "params" -> List(Map("status" -> 500, "method" -> "POST",
            "rid" -> "experimentService", "title" -> "experimentService")), "data" -> ex.getMessage)),
          "ExperimentService")
        InternalServerError(
          JSONUtils.serialize(CommonUtil.errorResponse("analytics.device-register",
            ex.getMessage, "ERROR"))
        ).withHeaders(CONTENT_TYPE -> "application/json")
      }
    }
  }
}
