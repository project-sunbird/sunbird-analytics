package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
import org.ekstep.analytics.job.AuditComputationJob

class TestPipelineFailedEventsDailyAudit extends SparkSpec(null) {

  it should "Invoke Audit Check for PipelineAudit" in {
    val config = JobConfig(search = Fetcher(`type` = "none", query = None, queries = None),
      null, null, model = "org.ekstep.analytics.model.AuditComputationModel",
      modelParams = Some(Map("startDate" -> "2019-08-17", "endDate" -> "2019-08-18", "auditRules" -> s"""[{"name": "PipelineFailedEventsAudit", "description": "Pipeline Failed events Audit", "threshold": 20, "model": "org.ekstep.analytics.audit.PipelineFailedEventsDailyAudit", "priority": 1, "search": [{"type": "local", "queries": [{ "file":"src/test/resources/pipeline-failed-audit/pipeline-failed-events.log"}]}] }]""")),
      output = Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
      parallelization = Option(10), appName = Option("TestAuditComputationModel"))
    AuditComputationJob.main(JSONUtils.serialize(config))(Option(sc))
  }
}
