import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._
import org.ekstep.analytics.api.util.JSONUtils

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends BaseSpec {

    "Application" should {

        "send 404 on a bad request" in new WithApplication {
            route(FakeRequest(GET, "/boum")) must beSome.which (status(_) == NOT_FOUND)
        }

       "return api health status report - successful response" in new WithApplication {
           val response = route(FakeRequest(GET, "/health")).get
            status(response) must equalTo(OK)
       }
       
       "return error response on invalid request(recommendations) - error response" in new WithApplication {
    		val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """
    		post("/content/recommend", request)
			val response = post("/content/recommend", request);
//    		hasClientError(response);
       }
       
       "return the recommendations - successful response" in new WithApplication {
			val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}, "filters": {"contentType": "Story"}}} """
			post("/content/recommend", request)
			val response = post("/content/recommend", request);
			isOK(response);
       }
       
       "register tag" in new WithApplication {
    	   	val request = """ {} """
			val response = post("/metrics/tag/register/4f04da601e244d31aa7b1daf91c46341",request);
			isOK(response);
       }
       
       "un-register tag" in new WithApplication {
    	   	val response = route(FakeRequest(DELETE, "/metrics/tag/delete/4f04da601e244d31aa7b1daf91c46341")).get;
			isOK(response);
       }
       
       "recommendations - should return empty response when recommendations disabled" in new WithApplication {
			val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}, "filters": {"contentType": "Story"}}} """
			post("/content/recommend", request)
			val response = post("/content/recommend", request);
			isOK(response);
			contentAsString(response) must contain(""""count":0""")
       }
    }
}
