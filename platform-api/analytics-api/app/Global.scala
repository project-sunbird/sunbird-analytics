import akka.actor.{ActorRef, Props}
import appconf.AppConf
import play.api._
import play.api.mvc._
import filter.RequestInterceptor
import org.ekstep.analytics.api.service.experiment.{ExperimentResolver, ExperimentService}
import org.ekstep.analytics.api.service.experiment.Resolver.ModulusResolver
import org.ekstep.analytics.api.service.{DeviceRegisterService, SaveMetricsActor}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, RedisUtil}

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        APILogger.init("org.ekstep.analytics-api")
        // Logger.info("Caching content")
        // val config: Config = play.Play.application.configuration.underlying()
        // CacheUtil.initCache()(config)
        Logger.info("Application has started...")
        val metricsActor: ActorRef = app.actorSystem.actorOf(Props[SaveMetricsActor])
        val deviceRegsiterActor = app.actorSystem.actorOf(Props(new DeviceRegisterService(metricsActor)), "deviceRegisterServiceAPIActor")
        AppConf.setActorRef("deviceRegisterService", deviceRegsiterActor)

        // experiment Service
        ExperimentResolver.register(new ModulusResolver())
        val redisUtil = new RedisUtil()
        val elasticsearchService = new ElasticsearchService()
        val experimentActor = app.actorSystem.actorOf(Props(new ExperimentService(redisUtil, elasticsearchService)), "experimentActor")
        AppConf.setActorRef("experimentService", experimentActor)
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

}