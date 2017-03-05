package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils

class TestConsumptionMetricsUpdater extends SparkSpec(null) {
	"ConsumptionMetricsUpdater" should "execute the job" in {
		val config = JobConfig(Fetcher("local", None, None), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType"-> "ALL", "periodUpTo" -> 10.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
		val strConfig = JSONUtils.serialize(config);
		println(strConfig);
		ConsumptionMetricsUpdater.main(strConfig)(Option(sc));
	}
}