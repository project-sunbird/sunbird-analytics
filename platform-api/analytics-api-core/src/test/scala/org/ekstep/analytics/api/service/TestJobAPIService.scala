package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.SparkSpec
import com.typesafe.config.ConfigFactory

class TestJobAPIService extends SparkSpec {
	
	implicit val config = ConfigFactory.load();
	
	"JobAPIService" should "return response for data request" in {
		val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"start_date":"2016-11-01","end_date":"2016-11-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
		val result = JobAPIService.dataRequest(request);
		println("result:", result);
	}
	
	it should "return response for get data request" in {
		val result = JobAPIService.getDataRequest("dev-portal", "14621312DB7F8ED99BA1B16D8B430FAC");
		println("result:", result);
	}
  
}