package org.ekstep.analytics.model


import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, V3MetricEdata}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.job.Metrics.MetricsAuditJob
import org.ekstep.analytics.util.SecorMetricUtil
import org.scalamock.scalatest.MockFactory

class TestMetricsAuditModel extends SparkSpec(null) with MockFactory{

  "TestMetricsAuditJob" should "get the metrics for monitoring the data pipeline" in {
    val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"telemetry-denormalized/raw/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"failed\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"failed/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}},{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"kafka\",\"params\":{\"brokerList\":\"localhost:9092\",\"topic\":\"metrics.topic\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
    val config = JSONUtils.deserialize[JobConfig](auditConfig)
    val reportConfig = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.MetricsAuditModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("MetricsAuditModel"))
    val pipelineMetrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
  }

  it should "execute MetricsAudit job and won't throw any Exception" in {
    val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"telemetry-denormalized/raw/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"failed\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"failed/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}},{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"kafka\",\"params\":{\"brokerList\":\"localhost:9092\",\"topic\":\"metrics.topic\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"

    val config_1= JSONUtils.deserialize[JobConfig](configString);
    val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ExperimentDefinitionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestExperimentDefinitionJob"))
    MetricsAuditJob.main(configString)(Option(sc));
  }

  it should "provide the result for the secor metrics" in {

    val configString ="""{"search":{"type":"none"},"model":"org.ekstep.analytics.model.MetricsAuditJob","modelParams":{"auditConfig":{"druid":{"models":{"telemetry-events":{"query":"druid.telemetryDatasource.count.query"},"summary-events":{"query":"druid.summaryDatasource.count.query"}}},"secor":{"models":{"channel":{"search":{"type":"azure","queries":[{"bucket":"dev-data-store","prefix":"raw/","startDate":"2019-11-06","endDate":"2019-11-06"}]}},"denorm":{"search":{"type":"azure","queries":[{"bucket":"dev-data-store","prefix":"telemetry-denormalized/raw/","startDate":"2019-11-06","endDate":"2019-11-06"}]}},"failed":{"search":{"type":"azure","queries":[{"bucket":"dev-data-store","prefix":"failed/","startDate":"2019-11-06","endDate":"2019-11-06"}]}}}}}},"output":[{"to":"file","params":{"file":"output/test_metrics.log"}}],"parallelization":8,"appName":"Metrics Audit"}""";
    val config = JSONUtils.deserialize[JobConfig](configString).modelParams
    val auditConfig = config.get("auditConfig").asInstanceOf[Map[String, AnyRef]]
    val auditMetrics = SecorMetricUtil.getSecorMetrics(auditConfig.get("secor").get.asInstanceOf[Map[String, AnyRef]])

    auditMetrics.map(f => {
      f.edata.asInstanceOf[Map[String, AnyRef]].get("metrics").get.asInstanceOf[List[V3MetricEdata]].map(edata => {
        if ("inputEvents".equals(edata.metric)) edata.value.shouldBe(Some(144102))
        if ("userDataRetreivedCount".equals(edata.metric)) edata.value.shouldBe(Some(495))
        if ("deviceDataRetreivedCount".equals(edata.metric)) edata.value.shouldBe(Some(0))
        if ("AnalyticsAPI".equals(edata.metric)) edata.value.shouldBe(Some(6650))
      })})
  }
}
