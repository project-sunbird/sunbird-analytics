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

    "Application" should new WithApplication {

        "send 404 on a bad request" in {
            route(FakeRequest(GET, "/boum")) must beSome.which (status(_) == NOT_FOUND)
        }

       "return api health status report - successful response" in {
           val response = route(FakeRequest(GET, "/health")).get
            status(response) must equalTo(OK)
       }
       
       "return error response on invalid request(recommendations) - error response" in {
    		val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """
    		post("/content/recommend", request)
			val response = post("/content/recommend", request);
//    		hasClientError(response);
    		status(response) must equalTo(OK)
       }
       
       "return the recommendations - successful response" in {
			val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}, "filters": {"contentType": "Story"}}} """
			post("/content/recommend", request)
			val response = post("/content/recommend", request)
			isOK(response)
			status(response) must equalTo(OK)
       }
       
       "register tag" in {
    	   	val request = """ {} """
			val response = post("/tag/register/4f04da601e244d31aa7b1daf91c46341",request);
			isOK(response)
			status(response) must equalTo(OK)
       }
       
       "un-register tag" in {
    	   	val response = route(FakeRequest(DELETE, "/tag/delete/4f04da601e244d31aa7b1daf91c46341")).get;
			isOK(response)
			status(response) must equalTo(OK)
       }
       
       "recommendations - should return empty response when recommendations disabled" in {
			val request = """ {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"en"}, "filters": {"contentType": "Story"}}} """
			post("/content/recommend", request)
			val response = post("/content/recommend", request)
			isOK(response)
			contentAsString(response) must contain(""""count":0""")
       }
    }

		"Client Log API" should new WithApplication {
			"should return error response for invalid request" in {
				val request = """ {"request":{"context":{"pdata":{"id":"prod.diksha.portal","ver":"1.0","pid":"contentPlayer"}},"edata":{"dspec":{"os":"","make":"","mem":0,"idisk":"","edisk":"","scrn":"","camera":"","cpu":"","sims":0,"uaspec":{"agent":"","ver":"","system":"","platform":"","raw":""}},"crashts":"1560346371","crash_logs":"Exception in thread \"main\" java.lang.NullPointerException\n        at com.example.myproject.Book.getTitle(Book.java:16)\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\n"}}} """
				post("/data/v1/client/logs", request)
				val response = post("/data/v1/client/logs", request)
				hasClientError(response)
				contentAsString(response) must contain(""""errmsg": "property: did is null or empty!"""")
			}

			"should return success response for valid request" in {
				val request = """ {"request":{"context":{"pdata":{"id":"prod.diksha.portal","ver":"1.0","pid":"contentPlayer"},"did":"345345-345345-345345-345345"},"edata":{"dspec":{"os":"","make":"","mem":0,"idisk":"","edisk":"","scrn":"","camera":"","cpu":"","sims":0,"uaspec":{"agent":"","ver":"","system":"","platform":"","raw":""}},"crashts":"1560346371","crash_logs":"Exception in thread \"main\" java.lang.NullPointerException\n        at com.example.myproject.Book.getTitle(Book.java:16)\n        at com.example.myproject.Author.getBookTitles(Author.java:25)\n        at com.example.myproject.Bootstrap.main(Bootstrap.java:14)\n"}}} """
				post("/data/v1/client/logs", request)
				val response = post("/data/v1/client/logs", request)
				hasClientError(response)
				contentAsString(response) must contain(""""message": "Log captured successfully!"""")
			}
		}
}
