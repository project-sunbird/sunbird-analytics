import play.api.mvc.Result
import scala.concurrent.Future

import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._
import org.ekstep.analytics.api.util.JSONUtils


class BaseSpec extends Specification {
	
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