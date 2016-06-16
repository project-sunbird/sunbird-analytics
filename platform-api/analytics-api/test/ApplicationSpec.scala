import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._

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

    }
}
