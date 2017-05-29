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
    
    // TODO: Enhance Test Cases with real time data. 
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
        cummAllCESumm.users_count should be(2)
        cummAllCESumm.avg_ts_session should be(16.0)
        
        /**
        * Period - DAY
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm1 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "20170522").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm1.total_sessions should be(2)
        cummAllCESumm1.total_ts should be(13.6)
        cummAllCESumm1.users_count should be(0)
        cummAllCESumm1.avg_ts_session should be(6.0)
        
        /**
        * Period - MONTH
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm2 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm2.total_sessions should be(2)
        cummAllCESumm2.total_ts should be(13.6)
        cummAllCESumm2.users_count should be(0)
        cummAllCESumm2.avg_ts_session should be(6.0)
        
        /**
        * Period - MONTH
        * content_id - all
        */ 
        val cummAllCESumm3 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "all").first
        cummAllCESumm3.total_sessions should be(5)
        cummAllCESumm3.total_ts should be(84.04)
        cummAllCESumm3.users_count should be(2)
        cummAllCESumm3.avg_ts_session should be(16.0)
    }

}