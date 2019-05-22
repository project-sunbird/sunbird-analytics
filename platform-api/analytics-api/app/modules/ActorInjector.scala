package modules

import com.google.inject.AbstractModule
import org.ekstep.analytics.api.service.SaveMetricsActor
import play.api.libs.concurrent.AkkaGuiceSupport

class ActorInjector extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    bindActor[SaveMetricsActor]("save-metrics-actor")
  }
}
