import akka.actor.{ActorRef, Props}
import appconf.AppConf
import com.typesafe.config.ConfigFactory
import play.api._
import play.api.mvc._
import filter.RequestInterceptor
import org.ekstep.analytics.api.service.experiment.{ExperimentResolver, ExperimentService}
import org.ekstep.analytics.api.service.experiment.Resolver.ModulusResolver
import org.ekstep.analytics.api.service.{CacheRefreshActor, DeviceProfileService, DeviceRegisterService, SaveMetricsActor}
import org.ekstep.analytics.api.util.{APILogger, ElasticsearchService, RedisUtil}

object Global extends WithFilters(RequestInterceptor) {

    val deviceRegisterRedisUtil = new RedisUtil()
    val deviceProfileRedisUtil = new RedisUtil()

    Runtime.getRuntime.addShutdownHook(new Thread {
        override def run = {
            deviceProfileRedisUtil.closePool()
            deviceRegisterRedisUtil.closePool()
        }
    })

    override def beforeStart(app: Application) {
        APILogger.init("org.ekstep.analytics-api")
        // Logger.info("Caching content")
        // val config: Config = play.Play.application.configuration.underlying()
        // CacheUtil.initCache()(config)
        Logger.info("Application has started...")
        val config = ConfigFactory.load()


        val metricsActor: ActorRef = app.actorSystem.actorOf(Props(new SaveMetricsActor(config)))

        val deviceRegisterActor = app.actorSystem
          .actorOf(Props(new DeviceRegisterService(metricsActor, config, deviceRegisterRedisUtil)).withDispatcher("device-register-actor"), "deviceRegisterServiceAPIActor")
        AppConf.setActorRef("deviceRegisterService", deviceRegisterActor)

        val deviceProfileActor = app.actorSystem
          .actorOf(Props(new DeviceProfileService(metricsActor, config, deviceProfileRedisUtil)).withDispatcher("device-profile-actor"), "deviceProfileServiceAPIActor")
        AppConf.setActorRef("deviceProfileService", deviceProfileActor)

        // experiment Service
        ExperimentResolver.register(new ModulusResolver())
        val elasticsearchService = new ElasticsearchService()
        val experimentActor = app.actorSystem.actorOf(Props(new ExperimentService(deviceRegisterRedisUtil, elasticsearchService)), "experimentActor")
        AppConf.setActorRef("experimentService", experimentActor)

        //location cache refresh Actor
        val locationCacheRefreshActor: ActorRef = app.actorSystem.actorOf(Props(new CacheRefreshActor(config)))
        AppConf.setActorRef("locationCacheRefreshService", locationCacheRefreshActor)
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

}