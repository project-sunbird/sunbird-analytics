package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants

class TestUpdateConceptSnapshotDB extends SparkSpec(null) {
  
    it should "update the concept snapshot updater db and check the updated fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.concept_snapshot_summary");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/concept-snapshot-updater/test_data1.json");
        val rdd1 = UpdateConceptSnapshotDB.execute(rdd, None);
        
        val snapshotData1 = sc.cassandraTable[ConceptSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY).collect

        // Check for DAY record
        val record1 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (20170426 == x.d_period) }.last
        record1.total_content_count should be(1)
        record1.total_content_count_start should be(record1.total_content_count)
        record1.live_content_count should be(1)
        record1.live_content_count_start should be(record1.live_content_count)
        record1.review_content_count should be(0)
        record1.review_content_count_start should be(record1.review_content_count)
        
        // Check for WEEK record
        val record2 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017717 == x.d_period) }.last
        record2.total_content_count should be(1)
        record2.total_content_count_start should be(record1.total_content_count)
        record2.live_content_count should be(1)
        record2.live_content_count_start should be(record1.live_content_count)
        record2.review_content_count should be(0)
        record2.review_content_count_start should be(record1.review_content_count)
        
         // Check for MONTH record
        val record3 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201704 == x.d_period) }.last
        record3.total_content_count should be(1)
        record3.total_content_count_start should be(record1.total_content_count)
        record3.live_content_count should be(1)
        record3.live_content_count_start should be(record1.live_content_count)
        record3.review_content_count should be(0)
        record3.review_content_count_start should be(record1.review_content_count)
        
        val rdd2 = loadFile[DerivedEvent]("src/test/resources/concept-snapshot-updater/test_data2.json");
        val rdd3 = UpdateConceptSnapshotDB.execute(rdd2, None);
        
        val snapshotData2 = sc.cassandraTable[ConceptSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY).collect
        
        // Check for new DAY record
        val record4 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (20170427 == x.d_period) }.last
        record4.total_content_count should be(10)
        record4.total_content_count_start should be(record4.total_content_count)
        record4.live_content_count should be(4)
        record4.live_content_count_start should be(record4.live_content_count)
        record4.review_content_count should be(2)
        record4.review_content_count_start should be(record4.review_content_count)
        
        // Check for same WEEK record
        val record5 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017717 == x.d_period) }.last
        record5.total_content_count should be(10)
        record5.total_content_count_start should be(1)
        record5.live_content_count should be(4)
        record5.live_content_count_start should be(1)
        record5.review_content_count should be(2)
        record5.review_content_count_start should be(0)
        
         // Check for same MONTH record
        val record6 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201704 == x.d_period) }.last
        record6.total_content_count should be(10)
        record6.total_content_count_start should be(1)
        record6.live_content_count should be(4)
        record6.live_content_count_start should be(1)
        record6.review_content_count should be(2)
        record6.review_content_count_start should be(0)
        
        // Check for new WEEK record
        val record7 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017719 == x.d_period) }.last
        record7.total_content_count should be(10)
        record7.total_content_count_start should be(record7.total_content_count)
        record7.live_content_count should be(4)
        record7.live_content_count_start should be(record7.live_content_count)
        record7.review_content_count should be(2)
        record7.review_content_count_start should be(record7.review_content_count)
        
        // Check for new MONTH record
        val record8 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201705 == x.d_period) }.last
        record8.total_content_count should be(10)
        record8.total_content_count_start should be(record8.total_content_count)
        record8.live_content_count should be(4)
        record8.live_content_count_start should be(record8.live_content_count)
        record8.review_content_count should be(2)
        record8.review_content_count_start should be(record8.review_content_count)
        
    }
}