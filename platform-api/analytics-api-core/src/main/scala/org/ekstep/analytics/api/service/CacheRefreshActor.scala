package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import javax.inject.Inject
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CacheRefreshActor @Inject()(cacheUtil: CacheUtil) extends Actor {

    private val logger = LogManager.getLogger("cache-refresh-logger")
    implicit val config: Config = ConfigFactory.load()

    override def preStart(): Unit = {
        val cacheRefreshInterval: Int = config.getInt("cache.refresh.time.interval.min")
        context.system.scheduler.schedule(initialDelay = 0.seconds, interval = cacheRefreshInterval.minutes, self, DeviceLocation)
    }

    def receive = {
        case DeviceLocation => cacheUtil.initDeviceLocationCache()
    }
}
