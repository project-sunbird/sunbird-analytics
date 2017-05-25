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
    "UpdateContentEditorMetricsDB" should "update the content editor usage db and check the fields" in {

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
        cummAllCESumm.m_total_sessions should be(5)
        cummAllCESumm.m_total_ts should be(84.04)
        cummAllCESumm.m_users_count should be(2)
        cummAllCESumm.m_avg_time_spent should be(16.0)
        
        /**
        * Period - DAY
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm1 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "20170522").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm1.m_total_sessions should be(2)
        cummAllCESumm1.m_total_ts should be(13.6)
        cummAllCESumm1.m_users_count should be(0)
        cummAllCESumm1.m_avg_time_spent should be(0)
        
        /**
        * Period - MONTH
        * content_id - do_2122315986551685121193
        */ 
        val cummAllCESumm2 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "do_2122315986551685121193").first
        cummAllCESumm2.m_total_sessions should be(2)
        cummAllCESumm2.m_total_ts should be(13.6)
        cummAllCESumm2.m_users_count should be(0)
        cummAllCESumm2.m_avg_time_spent should be(0)
        
        /**
        * Period - MONTH
        * content_id - all
        */ 
        val cummAllCESumm3 = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", "201705").where("d_content_id=?", "all").first
        cummAllCESumm3.m_total_sessions should be(5)
        cummAllCESumm3.m_total_ts should be(84.04)
        cummAllCESumm3.m_users_count should be(2)
        cummAllCESumm3.m_avg_time_spent should be(16.0)
    }

}