package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
import org.ekstep.analytics.job.AuditComputationJob

class TestAuditComputationModel extends SparkSpec(null) {

  it should "Invoke Audit Check for PipelineAudit" in {
    val config = JobConfig(search = Fetcher(`type` = "none", query = None, queries = None),
      null, null, model = "org.ekstep.analytics.model.AuditComputationModel",
      modelParams = Some(Map("startDate" -> "2019-08-17", "endDate" -> "2019-08-18", "auditRules" -> s"""[{"name": "PipelineAudit", "description": "PipelineAudit", "thresholds": "2", "model": "org.ekstep.analytics.audit.PipelineDailyAudit", "priority": 1}]""")),
      output = Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
      parallelization = Option(10), appName = Option("TestAuditComputationModel"))
      AuditComputationJob.main(JSONUtils.serialize(config))(Option(sc))
  }

}
