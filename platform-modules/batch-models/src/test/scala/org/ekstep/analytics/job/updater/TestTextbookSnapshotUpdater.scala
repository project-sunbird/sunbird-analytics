package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.updater.TextbookSnapshotSummary
import org.apache.spark.rdd.RDD

/**
 * @author mahesh
 */

class TestTextbookSnapshotUpdater extends SparkGraphSpec(null) {
 	
	"TextbookSnapshotUpdater" should "execute the job and shouldn't throw any exception" in {
		val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/template.json")), Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/asset.json"))))), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
    	val strConfig = JSONUtils.serialize(config);
		TextbookSnapshotUpdater.main(strConfig)(Option(sc));
	};
	
}