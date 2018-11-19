package org.ekstep.analytics.job.updater

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.model.SparkSpec

class TestDialCodeUsageUpdater extends SparkSpec(null) {

  it should "execute the job and shouldn't throw any exception" in {
    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/dialcode-usage-updater/dialcode-usage-summary.log"))))), None, None, "org.ekstep.analytics.updater.DialcodeUsageUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Dialcode usage updater Test"), Option(false))
    DialcodeUsageUpdater.main(JSONUtils.serialize(config))(Option(sc));
  }

  }
