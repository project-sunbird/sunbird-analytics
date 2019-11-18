package modules

import com.google.inject.AbstractModule
import org.ekstep.analytics.api.service.{CacheRefreshActor, DeviceRegisterService, ExperimentAPIService, SaveMetricsActor}
import org.ekstep.analytics.api.util.APILogger
import play.api.libs.concurrent.AkkaGuiceSupport

class ActorInjector extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    // Actor Binding
    bindActor[DeviceRegisterService]("device-register-actor")
    bindActor[ExperimentAPIService]("experiment-actor")
    bindActor[SaveMetricsActor]("save-metrics-actor")
    bindActor[CacheRefreshActor]("cache-refresh-actor")

    // Services

    APILogger.init("org.ekstep.analytics-api")
  }
}
