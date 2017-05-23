package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestPortalUsageSummaryModel extends SparkSpec(null) {
  
    "PortalSessionSummaryModel" should "generate 1 portal usage summary event" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_1.log");
        val rdd2 = PortalUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(1)
        val event1 = me(0);

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
    }
}