import play.api._
import play.api.mvc._
import filter.RequestInterceptor
import com.typesafe.config.Config
import org.ekstep.analytics.api.util.CacheUtil
import org.ekstep.analytics.api.util.APILogger

object Global extends WithFilters(RequestInterceptor) {

    override def beforeStart(app: Application) {
        APILogger.init("org.ekstep.analytics-api")
        Logger.info("Caching content")
        val config: Config = play.Play.application.configuration.underlying()
        //CacheUtil.initCache()(config)
        Logger.info("Application has started...")
    }

    override def onStop(app: Application) {
        Logger.info("Application shutdown...")
    }

}