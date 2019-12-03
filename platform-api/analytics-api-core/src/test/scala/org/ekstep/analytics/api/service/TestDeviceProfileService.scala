package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.Config
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{DeviceStateDistrict, H2DBUtil, RedisUtil}
import org.mockito.Mockito.when
import redis.clients.jedis.Jedis

import scala.concurrent.ExecutionContext

class TestDeviceProfileService extends BaseSpec {

  val deviceProfileServiceMock: DeviceProfileService = mock[DeviceProfileService]
  private implicit val system: ActorSystem = ActorSystem("device-profile-test-actor-system", config)
  private val configMock = mock[Config]
  val redisUtil: RedisUtil = mock[RedisUtil]
  val h2DBUtil: H2DBUtil = mock[H2DBUtil]
  val redisIndex: Int = config.getInt("redis.deviceIndex")
  implicit val executor: ExecutionContext =  scala.concurrent.ExecutionContext.global
  implicit val jedisConnection: Jedis = redisUtil.getConnection(redisIndex)
  private val deviceProfileService = TestActorRef(new DeviceProfileService(configMock, redisUtil, h2DBUtil)).underlyingActor

  override def beforeAll() {
    super.beforeAll()
  }


  "Resolve location for get device profile" should "return location details given an IP address" in {
    when(deviceProfileServiceMock.resolveLocationFromH2(ipAddress = "106.51.74.185"))
      .thenReturn(DeviceStateDistrict("Karnataka", "BANGALORE"))
    val deviceLocation = deviceProfileServiceMock.resolveLocationFromH2("106.51.74.185")
    deviceLocation.state should be("Karnataka")
    deviceLocation.districtCustom should be("BANGALORE")
  }

  "Resolve location for get device profile" should "return empty location if the IP address is not found" in {
    when(deviceProfileServiceMock.resolveLocationFromH2(ipAddress = "106.51.74.185"))
      .thenReturn(new DeviceStateDistrict())
    val deviceLocation = deviceProfileServiceMock.resolveLocationFromH2("106.51.74.185")
    deviceLocation.state should be("")
    deviceLocation.districtCustom should be("")
  }

}
