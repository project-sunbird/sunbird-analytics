import javax.inject.Inject

import com.kenshoo.play.metrics.{MetricsImpl, MetricsFilter, Metrics, MetricsFilterImpl}
import play.api.http.Status
import play.api.inject.Module
import play.api.{Configuration, Environment}

class APIMetricsFilter @Inject() (metrics: Metrics) extends MetricsFilterImpl(metrics) {

  // configure metrics prefix
  override def labelPrefix: String = "analytics-api";

  // configure status codes to be monitored. other status codes are labeled as "other"
  override def knownStatuses = Seq(Status.OK)
}

class MetricsModule extends Module {
  def bindings(environment: Environment, configuration: Configuration) = {
    Seq(
      bind[MetricsFilter].to[APIMetricsFilter].eagerly,
      bind[Metrics].to[MetricsImpl].eagerly
    )
  }
}