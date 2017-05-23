package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestPortalUsageSummaryModel extends SparkSpec(null) {

    "PortalUsageSummaryModel" should "generate 2 portal usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_1.log");
        val rdd2 = PortalUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(2)

        val event1 = me(1);
        event1.eid should be("ME_PORTAL_USAGE_SUMMARY");
        event1.mid should be("2F637E1B1B32566291FC86A56920B24F");
        event1.context.pdata.model should be("PortalUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170504);
        event1.dimensions.author_id.get should be("all");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(1);
        summary1.anonymous_total_ts should be(327.0);
        summary1.anonymous_total_sessions should be(1);
        summary1.anonymous_avg_session_ts should be(327.0);
        summary1.percent_new_users_count should be(50.0);
        summary1.ce_percent_sessions should be(25.0);
        summary1.ce_total_sessions should be(1);
        summary1.total_sessions should be(4);
        summary1.avg_session_ts should be(484.75);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1939.0);
        summary1.unique_users_count should be(2);
        summary1.unique_users.size should be(2);
        summary1.total_pageviews_count should be(60);

        // check for all anonymous sessions
        val event2 = me(0);

        event2.eid should be("ME_PORTAL_USAGE_SUMMARY");
        event2.mid should be("136D2CFF9BD6BF5F97F9424B023FBF20");
        event2.context.pdata.model should be("PortalUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.period.get should be(20170503);
        event2.dimensions.author_id.get should be("all");

        val summary2 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event2.edata.eks));
        summary2.new_user_count should be(0);
        summary2.anonymous_total_ts should be(1939.0);
        summary2.anonymous_total_sessions should be(4);
        summary2.anonymous_avg_session_ts should be(484.75);
        summary2.percent_new_users_count should be(0.0);
        summary2.ce_percent_sessions should be(25.0);
        summary2.ce_total_sessions should be(1);
        summary2.total_sessions should be(4);
        summary2.avg_session_ts should be(484.75);
        summary2.avg_pageviews should be(15.0);
        summary2.total_ts should be(1939.0);
        summary2.unique_users_count should be(0);
        summary2.unique_users.size should be(0);
        summary2.total_pageviews_count should be(60);
    }

    it should "generate 1 portal usage summary event where ce sessions count = 0" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_2.log");
        val rdd2 = PortalUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1)
        val event1 = me(0);

        event1.eid should be("ME_PORTAL_USAGE_SUMMARY");
        event1.mid should be("7409C6E0A9BD0CE5ECB501E01C22B287");
        event1.context.pdata.model should be("PortalUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170505);
        event1.dimensions.author_id.get should be("all");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(1);
        summary1.anonymous_total_ts should be(794.0);
        summary1.anonymous_total_sessions should be(2);
        summary1.anonymous_avg_session_ts should be(397.0);
        summary1.percent_new_users_count should be(50.0);
        summary1.ce_percent_sessions should be(0.0);
        summary1.ce_total_sessions should be(0);
        summary1.total_sessions should be(4);
        summary1.avg_session_ts should be(484.75);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1939.0);
        summary1.unique_users_count should be(2);
        summary1.unique_users.size should be(2);
        summary1.total_pageviews_count should be(60);
    }

    it should "generate 1 portal usage summary event where all are new visits" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_3.log");
        val rdd2 = PortalUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1)
        val event1 = me(0);

        event1.eid should be("ME_PORTAL_USAGE_SUMMARY");
        event1.mid should be("840F4B48BFC083BEC21B59AE2F82BFF1");
        event1.context.pdata.model should be("PortalUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170502);
        event1.dimensions.author_id.get should be("all");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(4);
        summary1.anonymous_total_ts should be(0.0);
        summary1.anonymous_total_sessions should be(0);
        summary1.anonymous_avg_session_ts should be(0.0);
        summary1.percent_new_users_count should be(100.0);
        summary1.ce_percent_sessions should be(0.0);
        summary1.ce_total_sessions should be(0);
        summary1.total_sessions should be(4);
        summary1.avg_session_ts should be(484.75);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1939.0);
        summary1.unique_users_count should be(4);
        summary1.unique_users.size should be(4);
        summary1.total_pageviews_count should be(60);
    }
}