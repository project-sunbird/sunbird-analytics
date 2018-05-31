import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import scala.concurrent.Future
import filter.RequestInterceptor
import org.ekstep.analytics.framework.util.RestUtil
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CacheUtil
import util.APILogger

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        APILogger.init("org.ekstep.analytics-api")
        Logger.info("Caching content")
        val config: Config = play.Play.application.configuration.underlying();
        //CacheUtil.initCache()(config);
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

}