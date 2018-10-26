package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTimeUtils
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class TestDeviceRegisterService extends BaseSpec {
  
    override def beforeAll() {
        super.beforeAll()
        val query = "TRUNCATE TABLE " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE
        DBUtil.session.execute(query);
    }
    
    "DeviceRegisterService" should "register given device" in {
      		val response = DeviceRegisterService.registerDevice("test-device-1", "10.6.0.16", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"channel":"test-channel","spec":{"cpu":"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)","make":"Micromax Micromax A065","os":"Android 4.4.2"}}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-1'"
        val row = DBUtil.session.execute(query).asScala.head;
        row.getString("state") should be("-")
        row.getString("district") should be("-")
	  }
    
    it should "register given device with IP" in {
      		val response = DeviceRegisterService.registerDevice("test-device-2", "106.51.74.185", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"channel":"test-channel","spec":{"cpu":"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)","make":"Micromax Micromax A065","os":"Android 4.4.2"}}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-2'"
        val row = DBUtil.session.execute(query).asScala.head;
        row.getString("state") should be("Karnataka")
        row.getString("district") should be("Bangalore")
	  }
    
    it should "register given device without IP" in {
      		val response = DeviceRegisterService.registerDevice("test-device-3", "", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"channel":"test-channel","spec":{"cpu":"abi:  armeabi-v7a  ARMv7 Processor rev 4 (v7l)","make":"Micromax Micromax A065","os":"Android 4.4.2"}}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-3'"
        val row = DBUtil.session.execute(query).asScala;
        row.size should be(0)
	  }
}