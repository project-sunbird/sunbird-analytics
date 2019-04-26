package filter

import java.util.UUID

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import play.api.mvc._
import org.ekstep.analytics.api.util.APILogger

object RequestInterceptor extends Filter {

    implicit val className = "org.ekstep.analytics-api"

    def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {

        val startTime = System.currentTimeMillis()
        val msgid = UUID.randomUUID().toString();
        request.headers.add(("msgid", msgid));
        val msg = s"${msgid} | Method: ${request.method} | Path: ${request.uri} | Remote Address: ${request.remoteAddress} " +
            s"| Domain=${request.domain} | Params: ${request.rawQueryString} " +
            s"| User-Agent: [${request.headers.get("user-agent").getOrElse("N/A")}]"
        play.Logger.of("accesslog").info(msg)
        next(request).map { result =>
            val endTime = System.currentTimeMillis
            val requestTime = endTime - startTime
            val key = play.api.routing.Router.Tags.RouteActionMethod
            val apiName = if (request.tags.isDefinedAt(key)) request.tags(play.api.routing.Router.Tags.RouteActionMethod) else ""
            val queryParamsData = List(request.queryString.map { case (k, v) => k -> v.mkString })
            val paramsData =  Map("status" -> result.header.status, "rid" -> apiName, "uip" -> request.remoteAddress, "title" -> apiName, "duration" -> requestTime, "protocol" -> "", "method" -> request.method,"category" -> "", "size" -> "") :: queryParamsData
            APILogger.log("ekstep.analytics-api", Option(Map("type" -> "api_access", "value" -> 0, "params" -> paramsData)), apiName)
            result.withHeaders("Request-Time" -> requestTime.toString)
        }
    }
}