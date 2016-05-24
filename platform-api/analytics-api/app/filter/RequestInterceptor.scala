package filter
import play.api.mvc._
import scala.concurrent.Future

object RequestInterceptor extends Filter {
    
    def apply(next: (RequestHeader) => Future[Result])(request: RequestHeader): Future[Result] = {
        val result = next(request)
        play.Logger.info(request + "\n\t => " + result)
        result
    }

}