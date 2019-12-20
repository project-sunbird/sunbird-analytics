package modules

import com.google.inject.AbstractModule
import org.ekstep.analytics.api.service.{DeviceProfileService, _}
import org.ekstep.analytics.api.util.APILogger
import play.api.libs.concurrent.AkkaGuiceSupport

class ActorInjector extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    // Actor Binding
    bindActor[DeviceRegisterService](name = "device-register-actor")
    bindActor[DeviceProfileService]("device-profile-actor")
    bindActor[ExperimentAPIService](name = "experiment-actor")
    bindActor[SaveMetricsActor](name = "save-metrics-actor")
    bindActor[CacheRefreshActor](name = "cache-refresh-actor")

    // Services
    APILogger.init("org.ekstep.analytics-api")
  }
}
