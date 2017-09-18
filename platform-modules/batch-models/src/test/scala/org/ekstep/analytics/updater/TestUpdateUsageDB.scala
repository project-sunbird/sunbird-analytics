/**
 * @author Jitendra Singh Sankhwar
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
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.job.ReplaySupervisor
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Dispatcher
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.util.BloomFilterUtil

class TestUpdateUsageDB extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        val tag1 = RegisteredTag("1375b1d70a66a0f2c22dd1872b98030cb7d9bacb", System.currentTimeMillis(), true)
        sc.makeRDD(Seq(tag1)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)
        val connector = CassandraConnector(sc.getConf)
        val session = connector.openSession()
        session.execute("TRUNCATE local_content_db.usage_summary_fact")
    }

    override def afterAll() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM local_content_db.registered_tags WHERE tag_id='1375b1d70a66a0f2c22dd1872b98030cb7d9bacb'");
        }
        super.afterAll();
    }

    "UpdateUsageDB" should "update the usage updater db and check the updated fields" in {

        val rdd = loadFile[DerivedEvent]("src/test/resources/usage-updater/us_1.log");
        val rdd2 = UpdateUsageDB.execute(rdd, None);

        // cumulative (period = 0)  
        val zeroPerContnetSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30079035").where("d_period=?", 0).where("d_tag=?", "all").where("d_user_id=?", "all").first
        zeroPerContnetSumm.m_total_ts should be(143.89)
        zeroPerContnetSumm.m_avg_interactions_min should be(22.1)
        zeroPerContnetSumm.m_total_interactions should be(53)
        zeroPerContnetSumm.m_total_sessions should be(4)
        zeroPerContnetSumm.m_avg_ts_session should be(35.97)
        zeroPerContnetSumm.m_total_users_count should be(3)
        zeroPerContnetSumm.m_total_content_count should be(0)
        zeroPerContnetSumm.m_total_devices_count should be(2)

        val zeroAcrossSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 0).where("d_tag=?", "all").where("d_user_id=?", "all").first()
        zeroAcrossSumm.m_total_ts should be(254949.17)
        zeroAcrossSumm.m_avg_interactions_min should be(19.35)
        zeroAcrossSumm.m_total_interactions should be(82242)
        zeroAcrossSumm.m_total_sessions should be(2150)
        zeroAcrossSumm.m_avg_ts_session should be(118.58)
        zeroAcrossSumm.m_total_users_count should be(147)
        zeroAcrossSumm.m_total_content_count should be(157)
        zeroAcrossSumm.m_total_devices_count should be(48)

        val zeroPerTagSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 0).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").where("d_user_id=?", "all").first
        zeroPerTagSumm.m_total_ts should be(1331.53)
        zeroPerTagSumm.m_avg_interactions_min should be(27.22)
        zeroPerTagSumm.m_total_interactions should be(604)
        zeroPerTagSumm.m_total_sessions should be(21)
        zeroPerTagSumm.m_avg_ts_session should be(63.41)
        zeroPerTagSumm.m_total_users_count should be(2)
        zeroPerTagSumm.m_total_content_count should be(11)
        zeroPerTagSumm.m_total_devices_count should be(2)

        val zeroPerTagContentSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "domain_9998").where("d_period=?", 0).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "all").first
        zeroPerTagContentSumm.m_total_ts should be(6.98)
        zeroPerTagContentSumm.m_avg_interactions_min should be(42.98)
        zeroPerTagContentSumm.m_total_interactions should be(5)
        zeroPerTagContentSumm.m_total_sessions should be(2)
        zeroPerTagContentSumm.m_avg_ts_session should be(3.49)
        zeroPerTagContentSumm.m_total_users_count should be(1)
        zeroPerTagContentSumm.m_total_content_count should be(0)
        zeroPerTagContentSumm.m_total_devices_count should be(1)
        
        val zeroPerTagUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 0).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        zeroPerTagUserSumm.m_total_ts should be(11196.66)
        zeroPerTagUserSumm.m_avg_interactions_min should be(16.35)
        zeroPerTagUserSumm.m_total_interactions should be(3051)
        zeroPerTagUserSumm.m_total_sessions should be(110)
        zeroPerTagUserSumm.m_avg_ts_session should be(101.79)
        zeroPerTagUserSumm.m_total_users_count should be(0)
        zeroPerTagUserSumm.m_total_content_count should be(26)
        zeroPerTagUserSumm.m_total_devices_count should be(1)
        
        val zeroPerContentUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30079226").where("d_period=?", 0).where("d_tag=?", "all").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        zeroPerContentUserSumm.m_total_ts should be(1409.69)
        zeroPerContentUserSumm.m_avg_interactions_min should be(4.47)
        zeroPerContentUserSumm.m_total_interactions should be(105)
        zeroPerContentUserSumm.m_total_sessions should be(9)
        zeroPerContentUserSumm.m_avg_ts_session should be(156.63)
        zeroPerContentUserSumm.m_total_users_count should be(0)
        zeroPerContentUserSumm.m_total_content_count should be(0)
        zeroPerContentUserSumm.m_total_devices_count should be(1)

        // day period

        val dayPerContnetSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30013486").where("d_period=?", 20160920).where("d_tag=?", "all").where("d_user_id=?", "all").first
        dayPerContnetSumm.m_total_ts should be(63.74)
        dayPerContnetSumm.m_avg_interactions_min should be(60.24)
        dayPerContnetSumm.m_total_interactions should be(64)
        dayPerContnetSumm.m_total_sessions should be(1)
        dayPerContnetSumm.m_avg_ts_session should be(63.74)
        dayPerContnetSumm.m_total_users_count should be(1)
        dayPerContnetSumm.m_total_content_count should be(0)
        dayPerContnetSumm.m_total_devices_count should be(1)

        val dayAcrossSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 20160920).where("d_tag=?", "all").where("d_user_id=?", "all").first
        dayAcrossSumm.m_total_ts should be(73234.46)
        dayAcrossSumm.m_avg_interactions_min should be(16.87)
        dayAcrossSumm.m_total_interactions should be(20588)
        dayAcrossSumm.m_total_sessions should be(496)
        dayAcrossSumm.m_avg_ts_session should be(147.65)
        dayAcrossSumm.m_total_users_count should be(47)
        dayAcrossSumm.m_total_content_count should be(100)
        dayAcrossSumm.m_total_devices_count should be(42)

        val dayPerTagSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 20160920).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").where("d_user_id=?", "all").first
        dayPerTagSumm.m_total_ts should be(1222.53)
        dayPerTagSumm.m_avg_interactions_min should be(29.35)
        dayPerTagSumm.m_total_interactions should be(598)
        dayPerTagSumm.m_total_sessions should be(20)
        dayPerTagSumm.m_avg_ts_session should be(61.13)
        dayPerTagSumm.m_total_users_count should be(2)
        dayPerTagSumm.m_total_content_count should be(11)
        dayPerTagSumm.m_total_devices_count should be(2)

        val dayPerTagContentSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30075798").where("d_period=?", 20160920).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").where("d_user_id=?", "all").first
        dayPerTagContentSumm.m_total_ts should be(118.66)
        dayPerTagContentSumm.m_avg_interactions_min should be(5.06)
        dayPerTagContentSumm.m_total_interactions should be(10)
        dayPerTagContentSumm.m_total_sessions should be(2)
        dayPerTagContentSumm.m_avg_ts_session should be(59.33)
        dayPerTagContentSumm.m_total_users_count should be(1)
        dayPerTagContentSumm.m_total_content_count should be(0)
        dayPerTagContentSumm.m_total_devices_count should be(1)

        val dayPerTagUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 20160920).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        dayPerTagUserSumm.m_total_ts should be(7732.45)
        dayPerTagUserSumm.m_avg_interactions_min should be(16.56)
        dayPerTagUserSumm.m_total_interactions should be(2134)
        dayPerTagUserSumm.m_total_sessions should be(65)
        dayPerTagUserSumm.m_avg_ts_session should be(118.96)
        dayPerTagUserSumm.m_total_users_count should be(0)
        dayPerTagUserSumm.m_total_content_count should be(22)
        dayPerTagUserSumm.m_total_devices_count should be(1)
        
        val dayPerContentUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30079226").where("d_period=?", 20160920).where("d_tag=?", "all").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        dayPerContentUserSumm.m_total_ts should be(688.45)
        dayPerContentUserSumm.m_avg_interactions_min should be(4.62)
        dayPerContentUserSumm.m_total_interactions should be(53)
        dayPerContentUserSumm.m_total_sessions should be(3)
        dayPerContentUserSumm.m_avg_ts_session should be(229.48)
        dayPerContentUserSumm.m_total_users_count should be(0)
        dayPerContentUserSumm.m_total_content_count should be(0)
        dayPerContentUserSumm.m_total_devices_count should be(1)

        // week period

        val weekPerContnetSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30031255").where("d_period=?", 2016733).where("d_tag=?", "all").where("d_user_id=?", "all").first
        weekPerContnetSumm.m_total_ts should be(3128.91)
        weekPerContnetSumm.m_avg_interactions_min should be(13.48)
        weekPerContnetSumm.m_total_interactions should be(703)
        weekPerContnetSumm.m_total_sessions should be(9)
        weekPerContnetSumm.m_avg_ts_session should be(347.66)
        weekPerContnetSumm.m_total_users_count should be(5)
        weekPerContnetSumm.m_total_content_count should be(0)
        weekPerContnetSumm.m_total_devices_count should be(1)

        val weekAcrossSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016733).where("d_tag=?", "all").where("d_user_id=?", "all").first
        weekAcrossSumm.m_total_ts should be(9614.45)
        weekAcrossSumm.m_avg_interactions_min should be(13.6)
        weekAcrossSumm.m_total_interactions should be(2180)
        weekAcrossSumm.m_total_sessions should be(59)
        weekAcrossSumm.m_avg_ts_session should be(162.96)
        weekAcrossSumm.m_total_users_count should be(6)
        weekAcrossSumm.m_total_content_count should be(11)
        weekAcrossSumm.m_total_devices_count should be(1)

        val weekPerTagSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016737).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "all").first
        weekPerTagSumm.m_total_ts should be(21238.86)
        weekPerTagSumm.m_avg_interactions_min should be(21.17)
        weekPerTagSumm.m_total_interactions should be(7493)
        weekPerTagSumm.m_total_sessions should be(279)
        weekPerTagSumm.m_avg_ts_session should be(76.12)
        weekPerTagSumm.m_total_users_count should be(7)
        weekPerTagSumm.m_total_content_count should be(73)
        weekPerTagSumm.m_total_devices_count should be(3)

        val weekPerTagContentSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30083384").where("d_period=?", 2016737).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "all").first
        weekPerTagContentSumm.m_total_ts should be(50.39)
        weekPerTagContentSumm.m_avg_interactions_min should be(26.2)
        weekPerTagContentSumm.m_total_interactions should be(22)
        weekPerTagContentSumm.m_total_sessions should be(2)
        weekPerTagContentSumm.m_avg_ts_session should be(25.2)
        weekPerTagContentSumm.m_total_users_count should be(1)
        weekPerTagContentSumm.m_total_content_count should be(0)
        weekPerTagContentSumm.m_total_devices_count should be(1)
        
        val weekPerTagUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016738).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        weekPerTagUserSumm.m_total_ts should be(11196.66)
        weekPerTagUserSumm.m_avg_interactions_min should be(16.35)
        weekPerTagUserSumm.m_total_interactions should be(3051)
        weekPerTagUserSumm.m_total_sessions should be(110)
        weekPerTagUserSumm.m_avg_ts_session should be(101.79)
        weekPerTagUserSumm.m_total_users_count should be(0)
        weekPerTagUserSumm.m_total_content_count should be(26)
        weekPerTagUserSumm.m_total_devices_count should be(1)
        
        val weekPerContentUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "domain_3996").where("d_period=?", 2016738).where("d_tag=?", "all").where("d_user_id=?", "af912b52-2ae1-4f33-a6df-45be7eda2ae3").first
        weekPerContentUserSumm.m_total_ts should be(69.49)
        weekPerContentUserSumm.m_avg_interactions_min should be(0.86)
        weekPerContentUserSumm.m_total_interactions should be(1)
        weekPerContentUserSumm.m_total_sessions should be(1)
        weekPerContentUserSumm.m_avg_ts_session should be(69.49)
        weekPerContentUserSumm.m_total_users_count should be(0)
        weekPerContentUserSumm.m_total_content_count should be(0)
        weekPerContentUserSumm.m_total_devices_count should be(1)

        // month period

        val monthAcrossSummAug = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201608).where("d_tag=?", "all").where("d_user_id=?", "all").first
        monthAcrossSummAug.m_total_ts should be(19713.06)
        monthAcrossSummAug.m_avg_interactions_min should be(12.01)
        monthAcrossSummAug.m_total_interactions should be(3945)
        monthAcrossSummAug.m_total_sessions should be(108)
        monthAcrossSummAug.m_avg_ts_session should be(182.53)
        monthAcrossSummAug.m_total_users_count should be(10)
        monthAcrossSummAug.m_total_content_count should be(11)
        monthAcrossSummAug.m_total_devices_count should be(1)

        val monthAcrossSummSept = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201609).where("d_tag=?", "all").where("d_user_id=?", "all").first
        monthAcrossSummSept.m_total_ts should be(235236.11)
        monthAcrossSummSept.m_avg_interactions_min should be(19.97)
        monthAcrossSummSept.m_total_interactions should be(78297)
        monthAcrossSummSept.m_total_sessions should be(2042)
        monthAcrossSummSept.m_avg_ts_session should be(115.2)
        monthAcrossSummSept.m_total_users_count should be(139)
        monthAcrossSummSept.m_total_content_count should be(157)
        monthAcrossSummSept.m_total_devices_count should be(48)

        val monthPerContnetSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30013486").where("d_period=?", 201609).where("d_tag=?", "all").where("d_user_id=?", "all").first
        monthPerContnetSumm.m_total_ts should be(63.74)
        monthPerContnetSumm.m_avg_interactions_min should be(60.24)
        monthPerContnetSumm.m_total_interactions should be(64)
        monthPerContnetSumm.m_total_sessions should be(1)
        monthPerContnetSumm.m_avg_ts_session should be(63.74)
        monthPerContnetSumm.m_total_users_count should be(1)
        monthPerContnetSumm.m_total_content_count should be(0)
        monthPerContnetSumm.m_total_devices_count should be(1)

        val monthPerTagSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201609).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").where("d_user_id=?", "all").first
        monthPerTagSumm.m_total_ts should be(1331.53)
        monthPerTagSumm.m_avg_interactions_min should be(27.22)
        monthPerTagSumm.m_total_interactions should be(604)
        monthPerTagSumm.m_total_sessions should be(21)
        monthPerTagSumm.m_avg_ts_session should be(63.41)
        monthPerTagSumm.m_total_users_count should be(2)
        monthPerTagSumm.m_total_content_count should be(11)
        monthPerTagSumm.m_total_devices_count should be(2)

        val monthPerTagContentSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30031115").where("d_period=?", 201609).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").where("d_user_id=?", "all").first
        monthPerTagContentSumm.m_total_ts should be(133.38)
        monthPerTagContentSumm.m_avg_interactions_min should be(18.89)
        monthPerTagContentSumm.m_total_interactions should be(42)
        monthPerTagContentSumm.m_total_sessions should be(5)
        monthPerTagContentSumm.m_avg_ts_session should be(26.68)
        monthPerTagContentSumm.m_total_users_count should be(1)
        monthPerTagContentSumm.m_total_content_count should be(0)
        monthPerTagContentSumm.m_total_devices_count should be(1)
        
        val monthPerTagUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016738).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").where("d_user_id=?", "9d595c31-23b0-4d58-b287-04efb2a3a42f").first
        monthPerTagUserSumm.m_total_ts should be(11196.66)
        monthPerTagUserSumm.m_avg_interactions_min should be(16.35)
        monthPerTagUserSumm.m_total_interactions should be(3051)
        monthPerTagUserSumm.m_total_sessions should be(110)
        monthPerTagUserSumm.m_avg_ts_session should be(101.79)
        monthPerTagUserSumm.m_total_users_count should be(0)
        monthPerTagUserSumm.m_total_content_count should be(26)
        monthPerTagUserSumm.m_total_devices_count should be(1)
        
        val monthPerContentUserSumm = sc.cassandraTable[MEUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.USAGE_SUMMARY_FACT).where("d_content_id=?", "domain_3996").where("d_period=?", 2016738).where("d_tag=?", "all").where("d_user_id=?", "af912b52-2ae1-4f33-a6df-45be7eda2ae3").first
        monthPerContentUserSumm.m_total_ts should be(69.49)
        monthPerContentUserSumm.m_avg_interactions_min should be(0.86)
        monthPerContentUserSumm.m_total_interactions should be(1)
        monthPerContentUserSumm.m_total_sessions should be(1)
        monthPerContentUserSumm.m_avg_ts_session should be(69.49)
        monthPerContentUserSumm.m_total_users_count should be(0)
        monthPerContentUserSumm.m_total_content_count should be(0)
        monthPerContentUserSumm.m_total_devices_count should be(1)
    }
}