import scala.concurrent.Future

import org.specs2.mutable.Specification

import com.typesafe.config.ConfigFactory

import play.api.libs.json.Json
import play.api.mvc.Result
import play.api.test.FakeHeaders
import play.api.test.FakeRequest
import play.api.test.Helpers.OK
import play.api.test.Helpers.POST
import play.api.test.Helpers.contentAsString
import play.api.test.Helpers.contentType
import play.api.test.Helpers.defaultAwaitTimeout
import play.api.test.Helpers.route
import play.api.test.Helpers.status


class BaseSpec extends Specification {
	
  implicit val config = ConfigFactory.load();
	def post(apiURL: String, request: String): Future[Result] = {
		route(FakeRequest(POST, apiURL, FakeHeaders(Seq(("content-type", "application/json"))), Json.toJson(Json.parse(request)))).get
	}
	
	def isOK(response: Future[Result]) {
		status(response) must equalTo(OK)
		contentType(response) must beSome.which(_ == "application/json")
		contentAsString(response) must contain(""""status":"successful"}""")
	}
	
	def hasClientError(response: Future[Result]) {
		status(response) must equalTo(OK)
		contentType(response) must beSome.which(_ == "application/json")
		contentAsString(response) must contain(""""err":"CLIENT_ERROR","status":"failed"""")
	}
}