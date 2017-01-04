package filter

import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


import context.Context
import play.api.mvc.Filter
import play.api.mvc.RequestHeader
import play.api.mvc.Result
import util.APILogger

object RequestInterceptor extends Filter {
  implicit val className = "org.ekstep.analytics-api"
  def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
    APILogger.init("org.ekstep.analytics-api")
    val startTime = System.currentTimeMillis()
    Context.checkSparkContext();
    val msgid = UUID.randomUUID().toString();
    request.headers.add(("msgid", msgid));
    val msg = s"${msgid} | Method: ${request.method} | Path: ${request.uri} | Remote Address: ${request.remoteAddress} " +
      s"| Domain=${request.domain} | Params: ${request.rawQueryString} " +
      s"| User-Agent: [${request.headers.get("user-agent").getOrElse("N/A")}]"
    play.Logger.of("accesslog").info(msg);    
    next(request).map { result =>
      val endTime = System.currentTimeMillis
      val requestTime = endTime - startTime
      val apiName = request.tags(play.api.routing.Router.Tags.RouteActionMethod)  
      APILogger.log("ekstep.analytics-api", Option(Map("rid" -> apiName, "uip" -> request.remoteAddress, "type" -> "API", "title" -> apiName, "category" -> "", "size" -> "", "duration" -> requestTime, "status" -> result.header.status, "protocol" -> "", "method" -> request.method, "action" -> "", "value" -> 0, "params" -> request.queryString.map { case (k, v) => k -> v.mkString })))
      result.withHeaders("Request-Time" -> requestTime.toString)
    }
  }
}