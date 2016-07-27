import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.Future
import filter.RequestInterceptor
import context.Context
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.api.service.RecommendationAPIService

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        Context.setSparkContext();
        Logger.info("Caching content")
        RecommendationAPIService.initCache()(Context.sc, Map("service.search.url" -> play.Play.application.configuration.getString("service.search.url")));
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Context.closeSparkContext();
        Logger.info("Application shutdown...")
    }

}