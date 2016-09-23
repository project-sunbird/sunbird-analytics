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

class TestContentUsageUpdater extends SparkSpec(null) {

    "Content Usage Updater" should "update the content usage updater db and check the updated fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_usage_summary_fact");
        }

        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-updater/content_usage_summaries.log");
        val rdd2 = UpdateContentUsageDB.execute(rdd, None);

        // cumulative (period = 0)  
        val zeroPerContnetSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30079035").where("d_period=?", 0).where("d_tag=?", "all").first
        zeroPerContnetSumm.m_total_ts should be(143.89)
        zeroPerContnetSumm.m_avg_interactions_min should be(22.1)
        zeroPerContnetSumm.m_total_interactions should be(53)
        zeroPerContnetSumm.m_total_sessions should be(4)
        zeroPerContnetSumm.m_avg_ts_session should be(35.97)

        val zeroAcrossSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 0).where("d_tag=?", "all").first
        zeroAcrossSumm.m_total_ts should be(261835.97)
        zeroAcrossSumm.m_avg_interactions_min should be(18.99)
        zeroAcrossSumm.m_total_interactions should be(82852)
        zeroAcrossSumm.m_total_sessions should be(2168)
        zeroAcrossSumm.m_avg_ts_session should be(120.77)

        val zeroPerTagSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 0).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").first
        zeroPerTagSumm.m_total_ts should be(1331.53)
        zeroPerTagSumm.m_avg_interactions_min should be(27.22)
        zeroPerTagSumm.m_total_interactions should be(604)
        zeroPerTagSumm.m_total_sessions should be(21)
        zeroPerTagSumm.m_avg_ts_session should be(63.41)

        val zeroPerTagContentSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "domain_9998").where("d_period=?", 0).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").first
        zeroPerTagContentSumm.m_total_ts should be(6.98)
        zeroPerTagContentSumm.m_avg_interactions_min should be(42.98)
        zeroPerTagContentSumm.m_total_interactions should be(5)
        zeroPerTagContentSumm.m_total_sessions should be(2)
        zeroPerTagContentSumm.m_avg_ts_session should be(3.49)

        // day period

        val dayPerContnetSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30013486").where("d_period=?", 20160920).where("d_tag=?", "all").first
        dayPerContnetSumm.m_total_ts should be(63.74)
        dayPerContnetSumm.m_avg_interactions_min should be(60.24)
        dayPerContnetSumm.m_total_interactions should be(64)
        dayPerContnetSumm.m_total_sessions should be(1)
        dayPerContnetSumm.m_avg_ts_session should be(63.74)

        val dayAcrossSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 20160920).where("d_tag=?", "all").first
        dayAcrossSumm.m_total_ts should be(71474.96)
        dayAcrossSumm.m_avg_interactions_min should be(16.5)
        dayAcrossSumm.m_total_interactions should be(19652)
        dayAcrossSumm.m_total_sessions should be(476)
        dayAcrossSumm.m_avg_ts_session should be(150.16)

        val dayPerTagSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 20160920).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").first
        dayPerTagSumm.m_total_ts should be(1222.53)
        dayPerTagSumm.m_avg_interactions_min should be(29.35)
        dayPerTagSumm.m_total_interactions should be(598)
        dayPerTagSumm.m_total_sessions should be(20)
        dayPerTagSumm.m_avg_ts_session should be(61.13)

        val dayPerTagContentSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30075798").where("d_period=?", 20160920).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").first
        dayPerTagContentSumm.m_total_ts should be(118.66)
        dayPerTagContentSumm.m_avg_interactions_min should be(5.06)
        dayPerTagContentSumm.m_total_interactions should be(10)
        dayPerTagContentSumm.m_total_sessions should be(2)
        dayPerTagContentSumm.m_avg_ts_session should be(59.33)

        // week period

        val weekPerContnetSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30031255").where("d_period=?", 2016733).where("d_tag=?", "all").first
        weekPerContnetSumm.m_total_ts should be(3128.91)
        weekPerContnetSumm.m_avg_interactions_min should be(13.48)
        weekPerContnetSumm.m_total_interactions should be(703)
        weekPerContnetSumm.m_total_sessions should be(9)
        weekPerContnetSumm.m_avg_ts_session should be(347.66)

        val weekAcrossSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016733).where("d_tag=?", "all").first
        weekAcrossSumm.m_total_ts should be(9614.45)
        weekAcrossSumm.m_avg_interactions_min should be(13.6)
        weekAcrossSumm.m_total_interactions should be(2180)
        weekAcrossSumm.m_total_sessions should be(59)
        weekAcrossSumm.m_avg_ts_session should be(162.96)

        val weekPerTagSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 2016733).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").first
        weekPerTagSumm.m_total_ts should be(165.78)
        weekPerTagSumm.m_avg_interactions_min should be(3.98)
        weekPerTagSumm.m_total_interactions should be(11)
        weekPerTagSumm.m_total_sessions should be(2)
        weekPerTagSumm.m_avg_ts_session should be(82.89)

        val weekPerTagContentSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_20043386").where("d_period=?", 2016733).where("d_tag=?", "c6ed6e6849303c77c0182a282ebf318aad28f8d1").first
        weekPerTagContentSumm.m_total_ts should be(165.78)
        weekPerTagContentSumm.m_avg_interactions_min should be(3.98)
        weekPerTagContentSumm.m_total_interactions should be(11)
        weekPerTagContentSumm.m_total_sessions should be(2)
        weekPerTagContentSumm.m_avg_ts_session should be(82.89)

        // month period

        val monthAcrossSummJun = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201606).where("d_tag=?", "all").first
        monthAcrossSummJun.m_total_ts should be(2713.58)
        monthAcrossSummJun.m_avg_interactions_min should be(7.92)
        monthAcrossSummJun.m_total_interactions should be(358)
        monthAcrossSummJun.m_total_sessions should be(10)
        monthAcrossSummJun.m_avg_ts_session should be(271.36)

        val monthAcrossSummJul = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201607).where("d_tag=?", "all").first

        monthAcrossSummJul.m_total_ts should be(4173.22)
        monthAcrossSummJul.m_avg_interactions_min should be(3.62)
        monthAcrossSummJul.m_total_interactions should be(252)
        monthAcrossSummJul.m_total_sessions should be(8)
        monthAcrossSummJul.m_avg_ts_session should be(521.65)

        val monthAcrossSummAug = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201608).where("d_tag=?", "all").first
        monthAcrossSummAug.m_total_ts should be(19713.06)
        monthAcrossSummAug.m_avg_interactions_min should be(12.01)
        monthAcrossSummAug.m_total_interactions should be(3945)
        monthAcrossSummAug.m_total_sessions should be(108)
        monthAcrossSummAug.m_avg_ts_session should be(182.53)

        val monthAcrossSummSept = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201609).where("d_tag=?", "all").first
        monthAcrossSummSept.m_total_ts should be(235236.11)
        monthAcrossSummSept.m_avg_interactions_min should be(19.97)
        monthAcrossSummSept.m_total_interactions should be(78297)
        monthAcrossSummSept.m_total_sessions should be(2042)
        monthAcrossSummSept.m_avg_ts_session should be(115.2)

        val monthPerContnetSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30013486").where("d_period=?", 201609).where("d_tag=?", "all").first
        monthPerContnetSumm.m_total_ts should be(63.74)
        monthPerContnetSumm.m_avg_interactions_min should be(60.24)
        monthPerContnetSumm.m_total_interactions should be(64)
        monthPerContnetSumm.m_total_sessions should be(1)
        monthPerContnetSumm.m_avg_ts_session should be(63.74)

        val monthPerTagSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "all").where("d_period=?", 201609).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").first
        monthPerTagSumm.m_total_ts should be(1331.53)
        monthPerTagSumm.m_avg_interactions_min should be(27.22)
        monthPerTagSumm.m_total_interactions should be(604)
        monthPerTagSumm.m_total_sessions should be(21)
        monthPerTagSumm.m_avg_ts_session should be(63.41)

        val monthPerTagContentSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30031115").where("d_period=?", 201609).where("d_tag=?", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb").first
        monthPerTagContentSumm.m_total_ts should be(133.38)
        monthPerTagContentSumm.m_avg_interactions_min should be(18.89)
        monthPerTagContentSumm.m_total_interactions should be(42)
        monthPerTagContentSumm.m_total_sessions should be(5)
        monthPerTagContentSumm.m_avg_ts_session should be(26.68)
    }

}