package modules

import akka.actor.ActorRef
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import org.ekstep.analytics.api.service.SaveMetricsActor
import play.api.libs.concurrent.AkkaGuiceSupport

class ActorInjector extends AbstractModule with AkkaGuiceSupport {
  override def configure(): Unit = {
    bindActor[SaveMetricsActor]("save-metrics-actor")
  }
}
