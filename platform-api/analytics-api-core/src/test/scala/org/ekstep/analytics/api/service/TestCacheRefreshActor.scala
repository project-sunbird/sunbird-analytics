package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}
import org.mockito.Mockito.{times, verify}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class TestCacheRefreshActor extends BaseSpec {

  private implicit val system: ActorSystem = ActorSystem("cache-refresh-test-actor-system", config)
  

  "Cache refresh actor" should "refresh the cache periodically" in {
    implicit val config: Config = ConfigFactory.load()
    val cacheUtilMock = mock[CacheUtil]
    val cacheRefreshActorRef = TestActorRef(new CacheRefreshActor(cacheUtilMock))

    cacheRefreshActorRef.tell(DeviceLocation(continentName = "Asia", countryCode = "IN", countryName = "India", stateCode = "KA",
      state = "Karnataka", subDivsion2 = "", city = "Bangalore",
      stateCustom = "Karnataka", stateCodeCustom = "29", districtCustom = ""), ActorRef.noSender)

    verify(cacheUtilMock, times(1)).initDeviceLocationCache()
  }

}
