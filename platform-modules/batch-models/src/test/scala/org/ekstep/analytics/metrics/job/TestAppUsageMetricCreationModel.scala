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
import org.ekstep.analytics.updater.UpdateAppUsageDB

class TestAppUsageMetricCreationModel extends SparkSpec(null) {
  
    "AppUsageMetricCreationModel" should "execute AppUsageMetricCreationModel successfully" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.app_usage_summary_fact");
        }
        
        val start_date = DateTime.now().minusHours(2).getMillis
        val rdd = loadFile[DerivedEvent]("src/test/resources/portal-usage-updater/test_data_1.log");
        UpdateAppUsageDB.execute(rdd, None);
        val end_date = DateTime.now().plusHours(5).getMillis
        
        val data = sc.parallelize(List(""))
        val rdd2 = AppUsageMetricCreationModel.execute(data, Option(Map("start_date" -> start_date.asInstanceOf[AnyRef], "end_date" -> end_date.asInstanceOf[AnyRef])));
    }
}