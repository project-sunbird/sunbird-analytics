package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import appconf.AppConf
import com.google.inject.Inject
import javax.inject.{Named, Singleton}
import org.ekstep.analytics.api.{Params, Request, RequestBody}
import org.ekstep.analytics.api.service.{DeviceRegisterService, RegisterDevice}
import org.ekstep.analytics.api.util.{APILogger, CommonUtil, JSONUtils}
import org.ekstep.analytics.framework.util.JobLogger
import play.api.libs.json
import play.api.libs.json.{JsResult, JsString, JsSuccess, JsValue, Json, Reads}
import play.api.mvc.Action

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(system: ActorSystem) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")

  def registerDevice(deviceId: String) = Action.async(parse.json) { implicit request =>
    val deviceRegisterServiceAPIActor = AppConf.getActorRef("deviceRegisterService")
    val successResponse = AppConf.successResponse
    val body: JsValue = request.body
    // The X-Forwarded-For header from Azure is in the format '61.12.65.222:33740, 61.12.65.222'
    val ipAddr = request.headers.get("X-Forwarded-For").map {
      x =>
        val ipArray = x.split(",")
        if (ipArray.length == 2) ipArray(1).trim else ipArray(0).trim
    }

    val headerIP = ipAddr.getOrElse("")
    val uaspec = request.headers.get("User-Agent")
    val ip_addr = (body \ "request" \ "ip_addr").asOpt[String]
    val fcmToken = (body \ "request" \ "fcmToken").asOpt[String]
    val producer = (body \ "request" \ "producer").asOpt[String]
    val dspec: Option[String] = (body \ "request" \ "dspec").toOption  match {
      case Some(value) => Option(Json.stringify(value))
      case None => None
    }

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, headerIP, ip_addr, fcmToken, producer, dspec, uaspec), ActorRef.noSender)

    Future {
      Ok(successResponse)
        .withHeaders(CONTENT_TYPE -> "application/json")
    }
  }

}
