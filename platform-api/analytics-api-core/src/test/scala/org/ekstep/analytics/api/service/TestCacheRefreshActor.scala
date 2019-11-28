package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{CacheUtil, DeviceLocation}
import org.mockito.Mockito.{times, verify}

class TestCacheRefreshActor extends BaseSpec {

  private implicit val system: ActorSystem = ActorSystem("cache-refresh-test-actor-system", config)
  val cacheUtilMock = mock[CacheUtil]

  "Cache refresh actor" should "refersh the cache periodically" in {

    val cacheRefreshActorRef = TestActorRef(new CacheRefreshActor(cacheUtilMock))

    cacheRefreshActorRef.tell(DeviceLocation(continentName = "Asia", countryCode = "IN", countryName = "India", stateCode = "KA",
      state = "Karnataka", subDivsion2 = "", city = "Bangalore",
      stateCustom = "Karnataka", stateCodeCustom = "29", districtCustom = ""), ActorRef.noSender)

    verify(cacheUtilMock, times(1)).initDeviceLocationCache()
  }

}
