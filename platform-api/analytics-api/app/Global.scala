import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.Future
import filter.RequestInterceptor
import context.Context
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.api.service.RecommendationAPIService
import com.typesafe.config.Config

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        Context.setSparkContext();
        Logger.info("Caching content")
        val config: Config = play.Play.application.configuration.underlying();
//        val config = Map("service.search.url" -> play.Play.application.configuration.getString("service.search.url"),
//        				"service.search.path" -> play.Play.application.configuration.getString("service.search.path"),
//        				"service.search.requestbody" -> play.Play.application.configuration.getString("service.search.requestbody"),
//        				"service.search.limit" -> play.Play.application.configuration.getString("service.search.limit"));
        RecommendationAPIService.initCache()(Context.sc, config);
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Context.closeSparkContext();
        Logger.info("Application shutdown...")
    }

}