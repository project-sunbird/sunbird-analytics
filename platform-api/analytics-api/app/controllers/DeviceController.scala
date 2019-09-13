package controllers

import akka.actor._
import appconf.AppConf
import com.google.inject.Inject
import org.ekstep.analytics.api.service.RegisterDevice
import org.ekstep.analytics.api.util.{APILogger, CommonUtil, JSONUtils}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.{Action, Result}
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigFactory}
import org.ekstep.analytics.api.service.experiment.{ExperimentData, ExperimentRequest}

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(system: ActorSystem) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")
  lazy val configuration: Config = ConfigFactory.load()
  lazy val isExperimentEnabled: Boolean = configuration.getBoolean("deviceRegisterAPI.experiment.enable")

  def registerDevice(deviceId: String) = Action.async(parse.json) { implicit request =>
    val deviceRegisterServiceAPIActor: ActorRef = AppConf.getActorRef("deviceRegisterService")

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
    val firstAccess = (body \ "request" \ "first_access").asOpt[Long]
    val dspec: Option[String] = (body \ "request" \ "dspec").toOption.map {
      value => Json.stringify(value)
    }

    val extMap: Option[Map[String, String]] = (body \ "request" \ "ext").toOption.map {
      value => {
        val userId = (value \ "userid").asOpt[String]
        val url = (value \ "url").asOpt[String]
        val userIdMap = userId.map { data =>  Map("userId" -> data) }
        val urlMap = url.map { data => Map("url" -> data) }
        userIdMap.getOrElse(Map()) ++ urlMap.getOrElse(Map())
      }
    }

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, headerIP, ipAddr, fcmToken, producer, dspec, uaspec, firstAccess), ActorRef.noSender)

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
    val experimentActor: ActorRef = AppConf.getActorRef("experimentService")

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
}
