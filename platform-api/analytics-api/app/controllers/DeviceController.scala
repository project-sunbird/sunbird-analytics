package controllers

import akka.actor.{ActorRef, ActorSystem}
import appconf.AppConf
import com.google.inject.Inject
import org.ekstep.analytics.api.service.{RegisterDevice}
import org.ekstep.analytics.api.util.{ CommonUtil, JSONUtils}
import play.api.libs.json.{JsValue, Json}
import play.api.mvc.Action

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(system: ActorSystem) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")

  def registerDevice(deviceId: String) = Action.async(parse.json) { implicit request =>
    val deviceRegisterServiceAPIActor = AppConf.getActorRef("deviceRegisterService")
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

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, headerIP, ipAddr, fcmToken, producer, dspec, uaspec), ActorRef.noSender)

    // mock response, it should be replaced with actual implementation
    val experimentData = Map("id" -> "experiment-1",
          "name" -> "first-experiment",
          "endDate" -> "2019-08-09",
          "startDate" -> "2019-07-09",
          "launchConfig" -> Map("url" -> "http://xyz.com/page1", "key" -> "2342-2314-5345-1231")
      )

    Future {
      Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully", "experimentData" -> experimentData))))
        .withHeaders(CONTENT_TYPE -> "application/json")
    }
  }

}
