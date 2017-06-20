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
import org.ekstep.analytics.updater.UpdateContentEditorUsageDB

class TestCEUsageMetricCreationModel extends SparkSpec(null) {
  
    "CEUsageMetricCreationModel" should "execute CEUsageMetricCreationModel successfully" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.ce_usage_summary_fact");
        }
        
        val start_date = DateTime.now().toString(CommonUtil.dateFormat)
        val rdd = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-updater/ceus.log");
        UpdateContentEditorUsageDB.execute(rdd, None);
        
        val data = sc.parallelize(List(""))
        val rdd2 = CEUsageMetricCreationModel.execute(data, Option(Map("start_date" -> start_date.asInstanceOf[AnyRef], "end_date" -> start_date.asInstanceOf[AnyRef])));
    }
}