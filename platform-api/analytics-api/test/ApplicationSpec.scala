import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

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
            route(FakeRequest(GET, "/boum")) must beNone
        }
        
        "return hello world" in new WithApplication {
            val home = route(FakeRequest(GET, "/")).get
            status(home) must equalTo(OK)
        }

        "return the content usage metrics" in new WithApplication {
            val home = route(FakeRequest(POST, "/content/metrics/usage/test123", FakeHeaders(Seq("Content-type"->Seq("application/json"))), """ {"id": "ekstep.analytics.contentusagesummary", "ver": "1.0", "ts": "YYYY-MM-DDThh:mm:ssZ+/-nn.nn", "request": {"filter": {"partner_id": "org.ekstep.partner.pratham", "group_user": true }, "summaries": ["day","week","month","cumulative"], "trend": {"day": 7, "week": 5, "month": 12 } } } """)).get

            status(home) must equalTo(OK)
            contentType(home) must beSome.which(_ == "application/json")
        }

    }
}
