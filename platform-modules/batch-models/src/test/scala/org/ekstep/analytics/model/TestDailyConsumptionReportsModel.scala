package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
import org.ekstep.analytics.job.ConsumptionReportsJob

class TestDailyConsumptionReportsModel extends SparkSpec(null) {

  ignore should "Should run successfully" in {
    val config = JobConfig(search = Fetcher(`type` = "none", query = None, queries = None),
      null, null, model = "org.ekstep.analytics.model.DailyConsumptionReportModel",
      modelParams = Some(Map("adhoc_scripts_dir" -> "/tmp/telemetryreports/scripts",
        "adhoc_scripts_output_dir" -> "/tmp",
        "adhoc_scripts_virtualenv_dir" -> "/tmp/venv",
        "derived_summary_dir" -> "derived/wfs",
        "startDate" -> "2019-07-30", "endDate" -> "2019-07-30"
      )),
      output = Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))),
      parallelization = Option(10), appName = Option("TestDailyConsumptionReports"))
    ConsumptionReportsJob.main(JSONUtils.serialize(config))(Option(sc))
  }

}
