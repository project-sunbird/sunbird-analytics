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
            contentAsString(home) must contain(""""err":"SERVER_ERROR","status":"failed","errmsg":"Request cannot be blank"""")
        }

       "return api health status report - successful response" in new WithApplication {
           val home = route(FakeRequest(GET, "/health")).get
            status(home) must equalTo(OK)
       }
       
       "return the recommendations - successful response" in skipped {
			val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"YYYY-MM-DDThh:mm:ssZ+/-nn.nn","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"}}} """))
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			val content = JSONUtils.deserialize[Map[String, AnyRef]](contentAsString(home)).getOrElse("result", Map("content" -> List())).asInstanceOf[Map[String, AnyRef]].get("content").get.asInstanceOf[List[AnyRef]];
			status(home) must equalTo(OK)
			contentType(home) must beSome.which(_ == "application/json")
			content should not be empty
       }
       
       "retun only 2 contents - successful response" in skipped {
			val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"English"},"limit":3}} """));
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			val content = JSONUtils.deserialize[Map[String, AnyRef]](contentAsString(home)).getOrElse("result", Map("content" -> List())).asInstanceOf[Map[String, AnyRef]].get("content").get.asInstanceOf[List[AnyRef]];
			status(home) must equalTo(OK)
			contentType(home) must beSome.which(_ == "application/json")
			content should have size(3)
       }
       
       "return only 'Hindi' content - successful response" in new WithApplication {
    	   val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"Hindi"},"filters":{"language":"Hindi"},"limit":100}} """));
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			val content = JSONUtils.deserialize[Map[String, AnyRef]](contentAsString(home)).getOrElse("result", Map("content" -> List())).asInstanceOf[Map[String, AnyRef]].get("content").get.asInstanceOf[List[AnyRef]];
			status(home) must equalTo(OK)
			contentType(home) must beSome.which(_ == "application/json")
			content.filter { x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") } should have size(content.length)
       }
       
       "return only 'Hindi Stories' content - successful response" in new WithApplication {
    	   val req = Json.toJson(Json.parse(""" {"id":"ekstep.analytics.recommendations","ver":"1.0","ts":"2016-05-19T09:23:44.212+00:00","request":{"context":{"did":"5edf49c4-313c-4f57-fd52-9bfe35e3b7d6","dlang":"Hindi"},"filters":{"language":"Hindi", "contentType": "Story"},"limit":100}} """));
			val home = route(FakeRequest(POST, "/recommendations", FakeHeaders(Seq(("content-type", "application/json"))), req)).get
			val content = JSONUtils.deserialize[Map[String, AnyRef]](contentAsString(home)).getOrElse("result", Map("content" -> List())).asInstanceOf[Map[String, AnyRef]].get("content").get.asInstanceOf[List[AnyRef]];
			status(home) must equalTo(OK)
			contentType(home) must beSome.which(_ == "application/json")
			content
				.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("language", List()).asInstanceOf[List[String]].contains("Hindi") })
				.filter({ x => x.asInstanceOf[Map[String, AnyRef]].getOrElse("contentType", "").asInstanceOf[String].contains("Story") }) should have size(content.length)
       }
       
    }
}
