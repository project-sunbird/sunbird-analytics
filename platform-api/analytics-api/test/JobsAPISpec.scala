import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._
import org.ekstep.analytics.api.util.JSONUtils

@RunWith(classOf[JUnitRunner])
class JobsAPISpec extends BaseSpec {
  
	"Job Request API" should {
		"return result for data requst" in new WithApplication {
			val request = """{"id":"ekstep.analytics.data.out","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"filter":{"start_date":"2016-11-01","end_date":"2016-11-20","tags":["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]}}}""";
			val response = post("/data/request", request);
			isOK(response) 			
		}
		
//		"return result for get data request" in new WithApplication {
//			val response = get("/data/request/dev-portal/6a54bfa283de43a89086e69e2efdc9eb6750493d/read");
//		} 
	}
}