//import akka.stream.Materializer
//import javax.inject.Inject
//import play.api.http.Status
//import play.api.inject.Module
//import play.api.{Configuration, Environment}
//
//class APIMetricsFilter @Inject() (metrics: Metrics)(implicit val mat: Materializer) extends MetricsFilterImpl(metrics) {
//
//  // configure metrics prefix
//  override def labelPrefix: String = "analytics-api";
//
//  // configure status codes to be monitored. other status codes are labeled as "other"
//  override def knownStatuses = Seq(Status.OK)
//}
//
//class MetricsModule extends Module {
//  def bindings(environment: Environment, configuration: Configuration) = {
//    Seq(
//      bind[MetricsFilter].to[APIMetricsFilter].eagerly,
//      bind[Metrics].to[MetricsImpl].eagerly
//    )
//  }
//}