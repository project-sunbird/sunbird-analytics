package org.ekstep.analytics.api.service

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.logging.log4j.LogManager
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class CacheRefreshActor(config: Config) extends Actor {

    private val logger = LogManager.getLogger("cache-refresh-logger")

    override def preStart(): Unit = {
        val cacheRefreshInterval: Int = config.getInt("cache.refresh.time.interval.min")
        context.system.scheduler.schedule(initialDelay = 0.seconds, interval = cacheRefreshInterval.minutes, self, DeviceLocation)
    }

    def receive = {
        case DeviceLocation => CacheUtil.initDeviceLocationCache()(config)
    }
}
