/**
 * @author Sowmya Dixit
 */
package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdatePortalUsageDB extends SparkSpec(null) {
  
    "UpdatePortalUsageDB" should "update the app usage db and check the fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.app_usage_summary_fact");
        }
        
        val rdd = loadFile[DerivedEvent]("src/test/resources/portal-usage-updater/test_data_1.log");
        val rdd2 = UpdatePortalUsageDB.execute(rdd, None);

        // check for day record
        val dayRecord = sc.cassandraTable[PortalUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT).where("d_period=?", 20170504).where("d_author_id=?", "all").first
        dayRecord.anonymous_total_sessions should be(2)
        dayRecord.anonymous_total_ts should be(654.0)
        dayRecord.anonymous_avg_session_ts should be(327.0)
        dayRecord.total_sessions should be(6)
        dayRecord.total_ts should be(3224.0)
        dayRecord.avg_session_ts should be(537.33)
        dayRecord.ce_total_sessions should be(2)
        dayRecord.ce_percent_sessions should be(33.33)
        dayRecord.total_pageviews_count should be(90)
        dayRecord.avg_pageviews should be(15.0)
        dayRecord.unique_users_count should be(3)
        dayRecord.new_user_count should be(2)
        dayRecord.percent_new_users_count should be(66.67)
        
        // check for week record
        val weekRecord = sc.cassandraTable[PortalUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT).where("d_period=?", 2017718).where("d_author_id=?", "all").first
        weekRecord.anonymous_total_sessions should be(8)
        weekRecord.anonymous_total_ts should be(3387.0)
        weekRecord.anonymous_avg_session_ts should be(423.38)
        weekRecord.total_sessions should be(12)
        weekRecord.total_ts should be(6308.0)
        weekRecord.avg_session_ts should be(525.67)
        weekRecord.ce_total_sessions should be(2)
        weekRecord.ce_percent_sessions should be(16.67)
        weekRecord.total_pageviews_count should be(180)
        weekRecord.avg_pageviews should be(15.0)
        weekRecord.unique_users_count should be(5)
        weekRecord.new_user_count should be(5)
        weekRecord.percent_new_users_count should be(100.0)
        
        // check for month record
        val monthRecord = sc.cassandraTable[PortalUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT).where("d_period=?", 201705).where("d_author_id=?", "all").first
        monthRecord.anonymous_total_sessions should be(8)
        monthRecord.anonymous_total_ts should be(3387.0)
        monthRecord.anonymous_avg_session_ts should be(423.38)
        monthRecord.total_sessions should be(12)
        monthRecord.total_ts should be(6308.0)
        monthRecord.avg_session_ts should be(525.67)
        monthRecord.ce_total_sessions should be(2)
        monthRecord.ce_percent_sessions should be(16.67)
        monthRecord.total_pageviews_count should be(180)
        monthRecord.avg_pageviews should be(15.0)
        monthRecord.unique_users_count should be(5)
        monthRecord.new_user_count should be(5)
        monthRecord.percent_new_users_count should be(100.0)
        
        // check for cumulative record
        val cumRecord = sc.cassandraTable[PortalUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.APP_USAGE_SUMMARY_FACT).where("d_period=?", 0).where("d_author_id=?", "all").first
        cumRecord.anonymous_total_sessions should be(8)
        cumRecord.anonymous_total_ts should be(3387.0)
        cumRecord.anonymous_avg_session_ts should be(423.38)
        cumRecord.total_sessions should be(12)
        cumRecord.total_ts should be(6308.0)
        cumRecord.avg_session_ts should be(525.67)
        cumRecord.ce_total_sessions should be(2)
        cumRecord.ce_percent_sessions should be(16.67)
        cumRecord.total_pageviews_count should be(180)
        cumRecord.avg_pageviews should be(15.0)
        cumRecord.unique_users_count should be(5)
        cumRecord.new_user_count should be(5)
        cumRecord.percent_new_users_count should be(100.0)

    }
}