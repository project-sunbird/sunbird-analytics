package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.job.AuditComputationJob

class TestDruidVsPipelineEventsDailyAudit extends SparkSpec(null) {

  it should "Invoke Audit Check for PipelineAudit" in {

    val config = JobConfig(search = Fetcher(`type` = "none", query = None, queries = None),
      null, null, model = "org.ekstep.analytics.model.AuditComputationModel",
      modelParams = Some(Map("startDate" -> "2019-08-17", "endDate" -> "2019-08-18",
        "auditRules" ->
          s"""[{"name": "DruidVsPipelineEventsDailyAudit", "description": "Druid Vs Pipeline events Audit", "threshold": 20, "model": "org.ekstep.analytics.audit.DruidVsPipelineEventsDailyAudit", "priority": 1,
             |"params": { "syncMinusDays": 2, "syncPlusDays": 0 }, "startDate": "2019-08-17", "endDate": "2019-08-18",
             |"search": [{"type": "local", "queries": [{ "file":"src/test/resources/pipeline-events-count-audit/telemetry.log", "prefix": "telemetry-denormalized/raw/"}]},
             |{"type": "local", "queries": [{ "file":"src/test/resources/pipeline-events-count-audit/summary.log", "prefix": "telemetry-denormalized/summary/"}]}] }]""".stripMargin)),
      output = Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
      parallelization = Option(10), appName = Option("TestAuditComputationModel"))
    AuditComputationJob.main(JSONUtils.serialize(config))(Option(sc))
  }
}
