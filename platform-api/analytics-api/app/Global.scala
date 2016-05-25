import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.Future
import filter.RequestInterceptor
import context.Context

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        Context.setSparkContext();
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Context.closeSparkContext();
        Logger.info("Application shutdown...")
    }

    override def onBadRequest(request: RequestHeader, error: String) = {
        Future.successful(BadRequest("Bad Request: " + error))
    }

}