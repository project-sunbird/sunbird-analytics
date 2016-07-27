import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.Future
import filter.RequestInterceptor
import context.Context
import org.ekstep.analytics.framework.util.RestUtil
import content.Content

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        Context.setSparkContext();
        Content.setContent()(Context.sc);
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Context.closeSparkContext();
        Logger.info("Application shutdown...")
    }

}