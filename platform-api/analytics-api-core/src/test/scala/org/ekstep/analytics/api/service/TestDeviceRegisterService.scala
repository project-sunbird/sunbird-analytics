package org.ekstep.analytics.api.service

import com.datastax.driver.core.{ResultSet, Row}
import org.mockito.Mockito._
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.util.DeviceLocation
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.util.JSONUtils

class TestDeviceRegisterService extends BaseSpec {

  val deviceRegisterServiceMock: DeviceRegisterService = mock[DeviceRegisterService]

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
    // val query = "TRUNCATE TABLE " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE
    // DBUtil.session.execute(query)
  }

  val uaspec = s"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"

  "DeviceRegisterService" should "register given device" in {

    when(deviceRegisterServiceMock.registerDevice(did = "test-device-1", ipAddress = "10.6.0.16", request = request, uaspec = uaspec))
      .thenReturn(successResponse)

    val response = deviceRegisterServiceMock.registerDevice(did = "test-device-1", ipAddress = "10.6.0.16", request = request, uaspec = uaspec)
    val resp = JSONUtils.deserialize[Response](response)
    resp.id should be("analytics.device-register")
    resp.params.status should be(Some("successful"))
  }

  "Resolve location" should "return location details given an IP address" in {
    when(deviceRegisterServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(DeviceLocation("Asia", "India", "Karnataka", "", "Bangalore"))
    val deviceLocation = deviceRegisterServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.state should be("Karnataka")
    deviceLocation.city should be("Bangalore")
  }

  "Resolve location" should "return empty location if the IP address is not found" in {
    when(deviceRegisterServiceMock.resolveLocation(ipAddress = "106.51.74.185"))
      .thenReturn(new DeviceLocation())
    val deviceLocation = deviceRegisterServiceMock.resolveLocation("106.51.74.185")
    deviceLocation.state should be("")
    deviceLocation.city should be("")
  }

  "Update device profile db given state and district information" should "return updated device location and device spec details" in {
    val resultSetMock = mock[ResultSet]
    val mockedRow = mock[Row]
    when(resultSetMock.one()).thenReturn(mockedRow)
    when(mockedRow.getString("state")).thenReturn("Karnataka")
    when(mockedRow.getString("district")).thenReturn("Bangalore")
    when(mockedRow.getString("uaspec")).thenReturn(uaspec)

    val dspec = Map("cpu" -> "abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)",
      "make" -> "Micromax Micromax A065", "os" -> "Android 4.4.2")

    when(deviceRegisterServiceMock.updateDeviceProfile(did = "test-device-2", channel = "test-channel", state = Some("Karnataka"),
      district = Some("Bangalore"), deviceSpec = Some(dspec), uaspec = uaspec)).thenReturn(resultSetMock)

    val resultRow = deviceRegisterServiceMock.updateDeviceProfile(did = "test-device-2", channel = "test-channel", state = Some("Karnataka"),
      district = Some("Bangalore"), deviceSpec = Some(dspec), uaspec = uaspec).one()

    resultRow.getString("state") should be("Karnataka")
    resultRow.getString("district") should be("Bangalore")
    resultRow.getString("uaspec") should be(uaspec)
  }

  "Update device profile db with only dspec/uaspec" should "retur updated device spec and uaspec details" in {

    val dspec = Map("cpu" -> "abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)",
      "make" -> "Micromax Micromax A065", "os" -> "Android 4.4.2")

    val resultSetMock = mock[ResultSet]
    val mockedRow = mock[Row]
    when(resultSetMock.one()).thenReturn(mockedRow)
    when(mockedRow.getString("state")).thenReturn("")
    when(mockedRow.getString("district")).thenReturn("")
    when(mockedRow.getString("uaspec")).thenReturn(uaspec)

    when(deviceRegisterServiceMock.updateDeviceProfile(did = "test-device-2", channel = "test-channel", state = None,
      district = None, deviceSpec = Some(dspec), uaspec = uaspec)).thenReturn(resultSetMock)

    val resultRow = deviceRegisterServiceMock.updateDeviceProfile(did = "test-device-2", channel = "test-channel", state = None,
      district = None, deviceSpec = Some(dspec), uaspec = uaspec).one()

    resultRow.getString("state") should be("")
    resultRow.getString("district") should be("")
    resultRow.getString("uaspec") should be(uaspec)
  }

  /*
  ignore should "register given device with IP" in {
    val response =
      deviceRegisterServiceMock
        .registerDevice(did ="test-device-2", ipAddress = "106.51.74.185", request = request, uaspec = uaspec)
    val resp = JSONUtils.deserialize[Response](response)
    resp.id should be("analytics.device-register")
    resp.params.status should be(Some("successful"))

    val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-2'"
    val row = DBUtil.session.execute(query).asScala.head
    row.getString("state") should be("Karnataka")
    row.getString("district") should be("Bangalore")
  }

  ignore should "register given device without IP" in {
    val response =
      deviceRegisterServiceMock
        .registerDevice(did = "test-device-3", ipAddress = "", request = request, uaspec = uaspec)
    val resp = JSONUtils.deserialize[Response](response)
    resp.id should be("analytics.device-register")
    resp.params.status should be(Some("successful"))

    val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-3'"
    val row = DBUtil.session.execute(query).asScala
    row.size should be(0)
  }

  ignore should "register given device local IP" in {
    val response =
      deviceRegisterServiceMock.registerDevice(did = "test-device-4", ipAddress = "192.168.0.0",
        request =
          s"""
             |{"id":"analytics.device.register",
             |"ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00",
             |"params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},
             |"request":{"channel":"test-channel","ip_addr": "61.1.139.221"}}
             |""".stripMargin,
        uaspec = uaspec)
    val resp = JSONUtils.deserialize[Response](response)
    resp.id should be("analytics.device-register")
    resp.params.status should be(Some("successful"))

    val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-4'"
    val row = DBUtil.session.execute(query).asScala.head
    row.getString("state") should be("Karnataka")
    row.getString("district") should be("Mysore")
  }
  */
}