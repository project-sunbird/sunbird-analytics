package org.ekstep.analytics.api.service

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.Config
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.{DeviceLocation, DeviceStateDistrict, RedisUtil}
import org.ekstep.analytics.framework.util.JSONUtils
import org.mockito.Mockito._
import scala.util.{Failure, Success}

class TestDeviceRegisterService extends BaseSpec {

  val deviceRegisterServiceMock: DeviceRegisterService = mock[DeviceRegisterService]
  private implicit val system: ActorSystem = ActorSystem("device-register-test-actor-system", config)
  private val configMock = mock[Config]
  val redisUtil = mock[RedisUtil]
  val saveMetricsActor = TestActorRef(new SaveMetricsActor(configMock))
  val redisIndex: Int = config.getInt("redis.deviceIndex")
  implicit val executor =  scala.concurrent.ExecutionContext.global
  implicit val jedisConnection = redisUtil.getConnection(redisIndex)
  private val deviceRegisterService = TestActorRef(new DeviceRegisterService(saveMetricsActor, configMock, redisUtil)).underlyingActor

  val request: String =
    s"""
       |{"id":"analytics.device.register",
       |"ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00",
       |"params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},
       |"request":{"channel":"test-channel",
       |"dspec":{"cpu":"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)","make":"Micromax Micromax A065","os":"Android 4.4.2"}}}
       |""".stripMargin

  val successResponse: String =
    s"""
       |{
       |  "id":"analytics.device-register",
       |  "ver":"1.0",
       |  "ts":"2018-11-08T10:16:27.512+00:00",
       |  "params":{
       |    "resmsgid":"79594dd2-ad13-44fd-8797-8a05ca5cac7b",
       |    "status":"successful",
       |    "client_key":null
       |  },
       |  "responseCode":"OK",
       |  "result":{
       |    "message":"Device registered successfully"
       |  }
       |}
     """.stripMargin

  override def beforeAll() {
    super.beforeAll()
  }

  val uaspec = s"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"


  "Device register request " should "generate data for logging device register request" in {

    val deviceLocation = DeviceLocation("Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "KARNATAKA", "29", "BANGALORE")
    val deviceId = "test-device-1"
    val deviceSpec = JSONUtils.deserialize[Map[String, AnyRef]]("{\"cpu\":\"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)\",\"make\":\"Micromax Micromax A065\",\"os\":\"Android 4.4.2\"}")
    val producerId = Some("prod.diksha.app")
    val fcmToken = Some("test-token")

    when(configMock.getInt("metrics.time.interval.min")).thenReturn(300)
    when(configMock.getString("postgres.table.geo_location_city.name")).thenReturn("geo_location_city.name")
    when(configMock.getString("postgres.table.geo_location_city_ipv4.name")).thenReturn("geo_location_city_ipv4.name")

    val deviceProfileLog = DeviceProfileLog(device_id = deviceId, location = deviceLocation, producer_id = producerId,
      device_spec = Some(deviceSpec), uaspec = Some(uaspec), fcm_token = fcmToken)
    val log = deviceRegisterService.generateDeviceRegistrationLogEvent(deviceProfileLog)
    val outputMap = JSONUtils.deserialize[Map[String, AnyRef]](log)

    val listOfExpectedKeys = List("device_id", "city", "country", "country_code", "state_code_custom", "state_code", "state", "state_custom",
      "district_custom", "first_access", "device_spec", "uaspec", "api_last_updated_on", "producer_id", "fcm_token")
    val diff = outputMap.keySet.diff(listOfExpectedKeys.toSet)

    // All the input keys should be written to log
    diff.size should be (0)
  }


  "Optional fields in the device register request " should " be skipped from the log" in {

    val deviceLocation = DeviceLocation("Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "KARNATAKA", "29", "BANGALORE")
    val deviceId = "test-device-1"
    val deviceSpec = JSONUtils.deserialize[Map[String, AnyRef]]("{\"cpu\":\"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)\",\"make\":\"Micromax Micromax A065\",\"os\":\"Android 4.4.2\"}")

    when(configMock.getInt("metrics.time.interval.min")).thenReturn(300)
    when(configMock.getString("postgres.table.geo_location_city.name")).thenReturn("geo_location_city.name")
    when(configMock.getString("postgres.table.geo_location_city_ipv4.name")).thenReturn("geo_location_city_ipv4.name")

    val deviceProfileLog = DeviceProfileLog(device_id = deviceId, location = deviceLocation, device_spec = Some(deviceSpec), uaspec = Some(uaspec))
    val log = deviceRegisterService.generateDeviceRegistrationLogEvent(deviceProfileLog)
    val outputMap = JSONUtils.deserialize[Map[String, AnyRef]](log)

    // All the optional keys not present in the input request should not be written to log
    outputMap.contains("fcm_token") should be (false)
    outputMap.contains("producer_id") should be (false)
    outputMap.contains("first_access") should be (false)
  }

  "Resolve location" should "return location details given an IP address" in {
    when(deviceRegisterServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(DeviceLocation("Asia", "IN", "India", "KA", "Karnataka", "", "Bangalore", "KARNATAKA", "29", "BANGALORE"))
    val deviceLocation = deviceRegisterServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.countryCode should be("IN")
    deviceLocation.countryName should be("India")
    deviceLocation.stateCode should be("KA")
    deviceLocation.state should be("Karnataka")
    deviceLocation.city should be("Bangalore")
    deviceLocation.stateCustom should be("KARNATAKA")
    deviceLocation.stateCodeCustom should be("29")
    deviceLocation.districtCustom should be("BANGALORE")
  }

  "Resolve location" should "return empty location if the IP address is not found" in {
    when(deviceRegisterServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(new DeviceLocation())
    val deviceLocation = deviceRegisterServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.countryCode should be("")
    deviceLocation.countryName should be("")
    deviceLocation.stateCode should be("")
    deviceLocation.state should be("")
    deviceLocation.city should be("")
    deviceLocation.stateCustom should be("")
    deviceLocation.stateCodeCustom should be("")
    deviceLocation.districtCustom should be("")
  }

  "When User-Agent is empty" should "return empty string for user agent map" in {
    when(deviceRegisterServiceMock.parseUserAgent(None)).thenReturn(None)
    val uaspecResult: Option[String] = deviceRegisterServiceMock.parseUserAgent(None)
    uaspecResult should be (None)
  }

  "Resolve location for get device profile" should "return location details given an IP address" in {
    when(deviceRegisterServiceMock.resolveLocationFromH2(ipAddress = "106.51.74.185"))
      .thenReturn(DeviceStateDistrict("Karnataka", "BANGALORE"))
    val deviceLocation = deviceRegisterServiceMock.resolveLocationFromH2("106.51.74.185")
    deviceLocation.state should be("Karnataka")
    deviceLocation.districtCustom should be("BANGALORE")
  }

  "Resolve location for get device profile" should "return empty location if the IP address is not found" in {
    when(deviceRegisterServiceMock.resolveLocationFromH2(ipAddress = "106.51.74.185"))
      .thenReturn(new DeviceStateDistrict())
    val deviceLocation = deviceRegisterServiceMock.resolveLocationFromH2("106.51.74.185")
    deviceLocation.state should be("")
    deviceLocation.districtCustom should be("")
  }

  "Resolve declared location for get device profile" should "return declared location details" in {
    when(redisUtil.getAllByKey("test-device"))
      .thenReturn(Option(Map("user_declared_state" -> "Karnataka", "user_declared_district" -> "BANGALORE")))
//    when(deviceRegisterServiceMock.resolveLocationFromH2(ipAddress = "106.51.74.185"))
//      .thenReturn(DeviceStateDistrict("Karnataka", "BANGALORE"))
//    val deviceLocation = deviceRegisterServiceMock.getDeviceProfile(GetDeviceProfile("test-device", "106.51.74.185"))
//
//    deviceLocation onComplete {
//      case Success(value) => {
//        value.get.userDeclaredLocation.get.state should be ("Karnataka")
//        value.get.userDeclaredLocation.get.district should be ("BANGALORE")
//        value.get.ipLocation.get.state should be ("Karnataka")
//        value.get.ipLocation.get.district should be ("BANGALORE")
//      }
//      case Failure(error) => error.printStackTrace()
//    }

    val deviceLocation = redisUtil.getAllByKey("test-device")
    deviceLocation.get.get("user_declared_state") should be("Karnataka")
    deviceLocation.get.get("user_declared_district") should be("BANGALORE")
  }

  "Resolve user declared location for get device profile" should "return empty location declared location is not found" in {
    when(redisUtil.getAllByKey("test-device"))
      .thenReturn(None)

    val deviceLocation = redisUtil.getAllByKey("test-device")
    deviceLocation.isEmpty should be(true)
  }
}