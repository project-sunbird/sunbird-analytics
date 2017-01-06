package filter
import play.api.mvc._
import scala.concurrent.Future
import context.Context
import java.util.UUID

object RequestInterceptor extends Filter {
	
    def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
        Context.checkSparkContext();
        val msgid = UUID.randomUUID().toString();
        request.headers.add(("msgid", msgid));
        val msg = s"${msgid} | Method: ${request.method} | Path: ${request.uri} | Remote Address: ${request.remoteAddress} " +
	      s"| Domain=${request.domain} | Params: ${request.rawQueryString} " +
	      s"| User-Agent: [${request.headers.get("user-agent").getOrElse("N/A")}]"
	    play.Logger.of("accesslog").info(msg);
        val result = next(request)
        result
    }
}