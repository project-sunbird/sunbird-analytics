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
class ApplicationSpec extends Specification {

    "Application" should {

        "send 404 on a bad request" in new WithApplication {
            route(FakeRequest(GET, "/boum")) must beSome.which (status(_) == NOT_FOUND)
        }

        "return the content usage metrics - successful response" in new WithApplication {
            val req = Json.toJson(Json.parse(""" {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"filter": {"partner_id": "org.ekstep.partner.pratham", "group_user": true }, "summaries": ["day","week","month","cumulative"], "trend": {"day": 7, "week": 5, "month": 12 } } } """))
            val home = route(FakeRequest(POST, "/content/metrics/usage/test123", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
            status(home) must equalTo(OK)
            contentType(home) must beSome.which(_ == "application/json")
            contentAsString(home) must contain(""""status":"successful"}""")
            contentAsString(home) must contain(""""trend":{"day":[],"week":[],"month":[]},"summaries":{}}}""")
        }

        "return the content usage metrics - error response" in new WithApplication {
            val req = Json.toJson(Json.parse(""" {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn" } """))
            val home = route(FakeRequest(POST, "/content/metrics/usage/test123", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
            status(home) must equalTo(OK)
            contentType(home) must beSome.which(_ == "application/json")
            contentAsString(home) must contain(""""err":"CLIENT_ERROR","status":"failed","errmsg":"Request cannot be blank"""")
        }

       "return api health status report - successful response" in new WithApplication {
           val home = route(FakeRequest(GET, "/health")).get
            status(home) must equalTo(OK)
       }
       
       "return error response on invalid request - error response" in new WithApplication {
    		val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{}}} """))
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			contentType(home) must beSome.which(_ == "application/json")
			contentAsString(home) must contain(""""err":"CLIENT_ERROR","status":"failed","errmsg":"did or dlang is missing."}""")
       }
       
       "return the recommendations - successful response" in new WithApplication {
			val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"}, "filters": {"contentType": "Story"}}} """))
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			val content = JSONUtils.deserialize[Map[String, AnyRef]](contentAsString(home)).getOrElse("result", Map("content" -> List())).asInstanceOf[Map[String, AnyRef]].get("content").get.asInstanceOf[List[AnyRef]];
			status(home) must equalTo(OK)
			contentType(home) must beSome.which(_ == "application/json")
       }
       
    }
}
