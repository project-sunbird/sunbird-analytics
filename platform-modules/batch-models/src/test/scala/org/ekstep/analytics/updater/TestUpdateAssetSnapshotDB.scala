package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdateAssetSnapshotDB extends SparkSpec(null) {
    
    "UpdateAssetSnapshotDB" should "update asset snapshot db" in {
        
//        CassandraConnector(sc.getConf).withSessionDo { session =>
//            session.execute("TRUNCATE content_db.asset_snapshot_summary");
//        }
        
        val rdd = loadFile[DerivedEvent]("src/test/resources/asset-snapshot-updater/test1.log");
        val rdd1 = UpdateAssetSnapshotDB.execute(rdd, None);
        
    }
}