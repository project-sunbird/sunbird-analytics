package org.ekstep.analytics.api.service

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestProbe}
import com.typesafe.config.Config
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{DeviceStateDistrict, H2DBUtil, RedisUtil}
import org.mockito.Mockito.{times, verify, when}

class TestDeviceProfileService extends BaseSpec {

  val deviceProfileServiceMock: DeviceProfileService = mock[DeviceProfileService]
  private implicit val system: ActorSystem = ActorSystem("device-register-test-actor-system", config)
  private val configMock = mock[Config]
  private val redisUtilMock = mock[RedisUtil]
  private val H2DBMock = mock[H2DBUtil]
  implicit val executor = scala.concurrent.ExecutionContext.global
  implicit val jedisConnection = redisUtilMock.getConnection(redisIndex)
  val redisIndex: Int = config.getInt("redis.deviceIndex")
  val saveMetricsActor = TestActorRef(new SaveMetricsActor)
  val metricsActorProbe = TestProbe()
  when(configMock.getString("postgres.table.geo_location_city.name")).thenReturn("geo_location_city")
  when(configMock.getString("postgres.table.geo_location_city_ipv4.name")).thenReturn("geo_location_city_ipv4")
  when(configMock.getBoolean("device.api.enable.debug.log")).thenReturn(true)
  private val deviceProfileServiceActorRef = TestActorRef(new DeviceProfileService(configMock, redisUtilMock, H2DBMock) {

  })
  val geoLocationCityIpv4TableName = config.getString("postgres.table.geo_location_city_ipv4.name")
  val geoLocationCityTableName = config.getString("postgres.table.geo_location_city.name")


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


  "Device profileService" should "get the device profile data" in {
    val query =
      s"""
         |SELECT
         |  glc.subdivision_1_name state,
         |  glc.subdivision_2_custom_name district_custom
         |FROM $geoLocationCityIpv4TableName gip,
         |  $geoLocationCityTableName glc
         |WHERE gip.geoname_id = glc.geoname_id
         |  AND gip.network_start_integer <= 1781746361
         |  AND gip.network_last_integer >= 1781746361
               """.stripMargin
    when(H2DBMock.readLocation(query)).thenReturn(DeviceStateDistrict("Karnataka", "Tumkur"))
    when(redisUtilMock.getAllByKey("device-001")).thenReturn(Some(Map("user_declared_state" -> "Karnatka", "user_declared_district" -> "Tumkur")))
    deviceProfileServiceActorRef.tell(DeviceProfileRequest("device-001", "106.51.74.185"), ActorRef.noSender)
    verify(H2DBMock, times(1)).readLocation(query)
  }

  "Device profileService" should "When state is not defined" in {

    val query =
      s"""
         |SELECT
         |  glc.subdivision_1_name state,
         |  glc.subdivision_2_custom_name district_custom
         |FROM $geoLocationCityIpv4TableName gip,
         |  $geoLocationCityTableName glc
         |WHERE gip.geoname_id = glc.geoname_id
         |  AND gip.network_start_integer <= 1781746361
         |  AND gip.network_last_integer >= 1781746361
               """.stripMargin
    when(H2DBMock.readLocation(query)).thenReturn(DeviceStateDistrict("", ""))
    when(redisUtilMock.getAllByKey("device-001")).thenReturn(Some(Map("user_declared_state" -> "Karnatka", "user_declared_district" -> "Tumkur")))
    deviceProfileServiceActorRef.tell(DeviceProfileRequest("device-001", "106.51.74.185"), ActorRef.noSender)
    verify(H2DBMock, times(2)).readLocation(query)
  }


  "Device profileService" should "catch the exception" in {
    intercept[Exception] {
      val query =
        s"""
           |SELECT
           |  glc.subdivision_1_name state,
           |  glc.subdivision_2_custom_name district_custom
           |FROM $geoLocationCityIpv4TableName gip,
           |  $geoLocationCityTableName glc
           |WHERE gip.geoname_id = glc.geoname_id
           |  AND gip.network_start_integer <= 1781746361
           |  AND gip.network_last_integer >= 1781746361
               """.stripMargin
      when(H2DBMock.readLocation(query)).thenThrow(new Exception("Error"))
      deviceProfileServiceActorRef.tell(DeviceProfileRequest("device-001", "106.51.74.185"), ActorRef.noSender)
    }
  }

}
