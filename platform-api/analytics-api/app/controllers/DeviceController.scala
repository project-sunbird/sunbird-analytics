package controllers

import akka.actor.{ActorRef, ActorSystem, Props}
import com.google.inject.Inject
import javax.inject.Named
import org.ekstep.analytics.api.service.{DeviceRegisterService, RegisterDevice, SaveMetricsActor}
import org.ekstep.analytics.api.util.{CommonUtil, JSONUtils}
import play.api.libs.json.Json
import play.api.mvc.Action

import scala.concurrent.{ExecutionContext, Future}

class DeviceController @Inject()(system: ActorSystem, @Named("save-metrics-actor") metricsActor: ActorRef) extends BaseController {

  implicit val ec: ExecutionContext = system.dispatchers.lookup("device-register-controller")
  // val metricsActor = system.actorOf(Props[SaveMetricsActor])
  private val deviceRegisterServiceAPIActor = system.actorOf(Props(new DeviceRegisterService(metricsActor)))

  def registerDevice(deviceId: String) = Action.async { implicit request =>
    val body: String = Json.stringify(request.body.asJson.get)
    // The X-Forwarded-For header from Azure is in the format '61.12.65.222:33740, 61.12.65.222'
    val ipAddr = request.headers.get("X-Forwarded-For").map {
      x =>
        val ipArray = x.split(",")
        if (ipArray.length == 2) ipArray(1).trim else ipArray(0).trim
    }
    val ip = ipAddr.getOrElse("")
    val uaspec = request.headers.get("User-Agent")

    deviceRegisterServiceAPIActor.tell(RegisterDevice(deviceId, ip, body, uaspec), ActorRef.noSender)

    Future {
      Ok(JSONUtils.serialize(CommonUtil.OK("analytics.device-register",
        Map("message" -> s"Device registered successfully"))))
        .withHeaders(CONTENT_TYPE -> "application/json")
    }
  }

}
