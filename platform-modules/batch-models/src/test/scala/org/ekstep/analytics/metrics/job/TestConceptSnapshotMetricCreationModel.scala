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
import org.ekstep.analytics.updater.UpdateConceptSnapshotDB

class TestConceptSnapshotMetricCreationModel extends SparkSpec(null) {
  
    "ConceptSnapshotMetricCreationModel" should "execute ConceptSnapshotMetricCreationModel successfully" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.concept_snapshot_summary");
        }
        
        val start_date = DateTime.now().toString(CommonUtil.dateFormat)
        val rdd = loadFile[DerivedEvent]("src/test/resources/concept-snapshot-updater/test_data1.json");
        UpdateConceptSnapshotDB.execute(rdd, None);
        
        val data = sc.parallelize(List(""))
        val rdd2 = ConceptSnapshotMetricCreationModel.execute(data, Option(Map("start_date" -> start_date.asInstanceOf[AnyRef], "end_date" -> start_date.asInstanceOf[AnyRef])));
    }
}