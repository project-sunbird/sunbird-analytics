import akka.actor.{ActorRef, Props}
import appconf.AppConf
import play.api._
import play.api.mvc._
import filter.RequestInterceptor
import org.ekstep.analytics.api.service.{DeviceRegisterService, SaveMetricsActor}
import org.ekstep.analytics.api.util.APILogger

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        APILogger.init("org.ekstep.analytics-api")
        // Logger.info("Caching content")
        // val config: Config = play.Play.application.configuration.underlying()
        // CacheUtil.initCache()(config)
        Logger.info("Application has started...")
        val metricsActor: ActorRef = app.actorSystem.actorOf(Props[SaveMetricsActor])
        val deviceRegsiterActor = app.actorSystem.actorOf(Props(new DeviceRegisterService(metricsActor)))
        AppConf.setActorRef("deviceRegisterService", deviceRegsiterActor)
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

}