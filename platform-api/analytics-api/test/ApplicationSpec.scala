import org.junit.runner._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._

/**
 * Add your spec here.
 * You can mock out a whole application including requests, plugins etc.
 * For more information, consult the wiki.
 */
@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends BaseSpec {

	"Application" should new WithApplication {
		"send 404 on a bad request" in {
			route(app, FakeRequest(GET, "/boum")) must beSome.which(status(_) == NOT_FOUND)
		}

		"return api health status report - successful response" in {
			val response = route(app, FakeRequest(GET, "/health")).get
			status(response) must equalTo(OK)
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
