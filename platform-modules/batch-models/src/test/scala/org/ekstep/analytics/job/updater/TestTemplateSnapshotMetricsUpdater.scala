package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.updater.TextbookSnapshotSummary
import org.apache.spark.rdd.RDD

class TestTemplateSnapshotMetricsUpdater extends SparkGraphSpec {

    "TemplateSnapshotMetricsUpdater" should "execute the job and shouldn't throw any exception" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/asset-snapshot-updater/test1.log"))))), None, None, "org.ekstep.analytics.updater.UpdatePluginSnapshotMetrics", Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestTemplateSnapshotMetricsUpdater"), Option(false))
        TemplateSnapshotMetricsUpdater.main(JSONUtils.serialize(config))(Option(sc));
    }

}