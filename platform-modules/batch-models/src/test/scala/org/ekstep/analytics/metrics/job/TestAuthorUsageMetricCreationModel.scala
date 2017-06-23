package org.ekstep.analytics.metrics.job

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.model.SparkSpec
import org.joda.time.DateTime
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.updater.UpdateAuthorSummaryDB

class TestAuthorUsageMetricCreationModel extends SparkSpec(null) {
  
    "AuthorSnapshotMetricCreationModel" should "execute AuthorSnapshotMetricCreationModel successfully" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.author_usage_summary_fact");
        }
        
        val start_date = DateTime.now().toString(CommonUtil.dateFormat)
        val input = loadFile[DerivedEvent]("src/test/resources/author-usage-updater/test.log");
        UpdateAuthorSummaryDB.execute(input, Option(Map()))

        val data = sc.parallelize(List(""))
        val rdd2 = AuthorUsageMetricCreationModel.execute(data, Option(Map("start_date" -> start_date.asInstanceOf[AnyRef], "end_date" -> start_date.asInstanceOf[AnyRef])));
    }
}