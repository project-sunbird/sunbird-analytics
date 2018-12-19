package org.ekstep.analytics.updater

import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import org.ekstep.analytics.util.WorkFlowUsageSummaryFact

class TestUpdateMetricsDB extends SparkSpec(null) {

  override def beforeAll() {
    super.beforeAll()
    val connector = CassandraConnector(sc.getConf);
    val session = connector.openSession();
    session.execute("TRUNCATE local_platform_db.workflow_usage_summary");
  }

  override def afterAll() {
    super.afterAll();
  }

  it should "update all usage suammary db and check the updated fields" in {
    val rdd = loadFile[DerivedEvent]("src/test/resources/workflow-usage-updater/test-data2.log");
    val rdd2 = UpdateMetrics.execute(rdd, None);
    rdd2.count() should be(1)

    val record1 = sc.cassandraTable[MetricsAlgoOutput](Constants.PLATFORM_KEY_SPACE_NAME, Constants.WORKFLOW_USAGE_SUMMARY).first()

    record1.total_content_play_sessions should be(0)
    record1.total_interactions should be(0)
    record1.total_timespent should be(0.0)
    record1.total_pageviews should be(0)

  }
}