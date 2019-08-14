package org.ekstep.analytics.job.audit

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.model.SparkSpec

class TestPipelineAuditJob extends SparkSpec(null) {

  "TestSimpleAuditJob" should "execute the job and shouldn't throw any exception" in {
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/audit-job/pipeline_metrics.txt"))))), None, None, "org.ekstep.analytics.job.audit.simpleAuditJob", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestSimpleAuditJob"), Option(false))
    PipelineAuditJob.main(JSONUtils.serialize(config))(Option(sc))
  }
}
