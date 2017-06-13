/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.updater

import scala.reflect.runtime.universe

import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdateContentEditorUsageDB extends SparkSpec(null) {
    
    "UpdateContentEditorUsageDB" should "update the content editor usage db and check the fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.ce_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-updater/ceus.log");
        val rdd2 = UpdateContentEditorUsageDB.execute(rdd, None);
        
        /**
        * Period - CUMMULATIVE
        * content_id - all
        */ 
        val cummAllCESumm = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", 0).where("d_content_id=?", "all").first
        cummAllCESumm.total_sessions should be(5)
        cummAllCESumm.total_ts should be(84.04)
        cummAllCESumm.unique_users_count should be(2)
        cummAllCESumm.avg_ts_session should be(16.81)
        
        /**
        * Period - DAY
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm1 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "20170522").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm1.total_sessions should be(2)
        cummAllCESumm1.total_ts should be(13.6)
        cummAllCESumm1.unique_users_count should be(0)
        cummAllCESumm1.avg_ts_session should be(6.8)
        
        /**
        * Period - MONTH
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm2 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm2.total_sessions should be(2)
        cummAllCESumm2.total_ts should be(13.6)
        cummAllCESumm2.unique_users_count should be(0)
        cummAllCESumm2.avg_ts_session should be(6.8)
        
        /**
        * Period - MONTH
        * content_id - all
        */ 
        val cummAllCESumm3 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "all").first
        cummAllCESumm3.total_sessions should be(5)
        cummAllCESumm3.total_ts should be(84.04)
        cummAllCESumm3.unique_users_count should be(2)
        cummAllCESumm3.avg_ts_session should be(16.81)
    }
    
    it should "update the content editor usage db and check the fields for content_id all" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-updater/ceus_1.log");
        val rdd2 = UpdateContentEditorUsageDB.execute(rdd, None);
        
        /**
        * Period - CUMMULATIVE
        * content_id - all
        */ 
        val cummAllCESumm = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", 0).where("d_content_id=?", "all").first
        cummAllCESumm.total_sessions should be(161)
        cummAllCESumm.total_ts should be(53598.58)
        cummAllCESumm.unique_users_count should be(31)
        cummAllCESumm.avg_ts_session should be(332.91)
        
    }
}