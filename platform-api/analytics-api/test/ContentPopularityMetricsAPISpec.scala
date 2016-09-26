import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._
import org.ekstep.analytics.api.util.JSONUtils

@RunWith(classOf[JUnitRunner])
class ContentPopularityMetricsAPISpec extends BaseSpec {
	
	val apiURL = "/metrics/content-popularity";
	
	"Content Popularity Metrics API" should {
		"return error response when, there is invalid request"  in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			hasClientError(response);
		}
		
		"return empty response when, no pre-computed tag summary data is there in S3 location" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
		
		"return one day metrics of last 7days when, only one day pre-computed tag summary data is there in S3 location for last 7days" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
		
		"return tag metrics when, last 7 days data present for tag1, tag2, tag3 in S3 & API inputs (tag: tag1)" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
		
		"return empty response when, last 7 days data present for tag1, tag2, tag3 in S3 & API input (tag: tag5)" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_7_DAYS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
		
		"return last 5 weeks metrics when, when 5 weeks data present" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_5_WEEKS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
		
		"return last 12 months metrics when, when 12 months data present" in new WithApplication {
			val request = """{"id":"ekstep.analytics.metrics.content-popularity","ver":"1.0","ts":"2016-09-12T18:43:23.890+00:00","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341"},"request":{"period":"LAST_12_MONTHS","filter":{"tag":"4f04da60-1e24-4d31-aa7b-1daf91c46341","content_id":"do_435543"}}}""";
			val response = post(apiURL, request);
			isOK(response)
		}
	}
}