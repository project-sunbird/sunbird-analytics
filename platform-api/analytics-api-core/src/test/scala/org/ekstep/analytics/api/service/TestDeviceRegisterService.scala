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
      		val response = DeviceRegisterService.registerDevice("test-device-1", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"loc":"17.26126126126126,75.60324418126551","ip_addr":"10.6.0.16","state":"Maharashtra","district":"Aurangabad","type":"app"}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-1'"
        val row = DBUtil.session.execute(query).asScala.head;
        row.getString("state") should be("-")
        row.getString("district") should be("-")
	  }
    
    it should "register given device with IP" in {
      		val response = DeviceRegisterService.registerDevice("test-device-2", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"loc":"17.26126126126126,75.60324418126551","ip_addr":"106.51.74.185","state":"Maharashtra","district":"Aurangabad","type":"app"}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-2'"
        val row = DBUtil.session.execute(query).asScala.head;
        row.getString("state") should be("Karnataka")
        row.getString("district") should be("Bangalore")
	  }
    
    it should "register given device without IP" in {
      		val response = DeviceRegisterService.registerDevice("test-device-3", """{"id":"analytics.device.register","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"loc":"17.26126126126126,75.60324418126551","state":"Maharashtra","district":"Aurangabad","type":"app"}}""");
      		val resp = JSONUtils.deserialize[Response](response);
        resp.id should be ("ekstep.analytics.device-register");
        resp.params.status should be (Some("successful"));
        
        val query = "SELECT * FROM " + Constants.DEVICE_DB + "." + Constants.DEVICE_PROFILE_TABLE + " WHERE device_id='test-device-3'"
        val row = DBUtil.session.execute(query).asScala.head;
        row.getString("state") should be("Maharashtra")
        row.getString("district") should be("Aurangabad")
	  }
}