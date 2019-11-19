package filter

import java.util.UUID

import akka.stream.Materializer
import javax.inject.Inject
import org.ekstep.analytics.api.util.APILogger
import play.api.mvc.{Filter, RequestHeader, Result}
import play.api.routing.{HandlerDef, Router}

import scala.concurrent.{ExecutionContext, Future}

class RequestInterceptor @Inject() (implicit val mat: Materializer, ec: ExecutionContext) extends Filter {

    implicit val className = "org.ekstep.analytics-api"

    def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {

        val startTime = System.currentTimeMillis()
        val msgid = UUID.randomUUID().toString
        request.headers.add(("msgid", msgid))
        val msg = s"${msgid} | Method: ${request.method} | Path: ${request.uri} | Remote Address: ${request.remoteAddress} " +
            s"| Domain=${request.domain} | Params: ${request.rawQueryString} " +
            s"| User-Agent: [${request.headers.get("user-agent").getOrElse("N/A")}]"
        play.Logger.of("accesslog").info(msg)
        next(request).map { result =>
            val endTime = System.currentTimeMillis
            val requestTime = endTime - startTime
            val handlerDef: HandlerDef = request.attrs(Router.Attrs.HandlerDef)
            val apiName = handlerDef.controller
            val queryParamsData = List(request.queryString.map { case (k, v) => k -> v.mkString })
            val paramsData =  Map("status" -> result.header.status, "rid" -> apiName, "title" -> apiName, "duration" -> requestTime, "protocol" -> "", "method" -> request.method,"category" -> "", "size" -> "") :: queryParamsData
            APILogger.log("ekstep.analytics-api", Option(Map("type" -> "api_access", "value" -> 0, "params" -> paramsData)), apiName)
            result.withHeaders("Request-Time" -> requestTime.toString)
        }
    }
}