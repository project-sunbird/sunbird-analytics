package filter
import play.api.mvc._
import scala.concurrent.Future
import context.Context

object RequestInterceptor extends Filter {
    
    def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
        Context.checkSparkContext();
        val result = next(request)
        result
    }

}