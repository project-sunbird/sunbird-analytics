/**
 * @author Sowmya Dixit
 **/
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent

class TestAppUsageSummaryModel extends SparkSpec(null) {

    "AppUsageSummaryModel" should "generate 4 app usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_1.log");
        val rdd2 = AppUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(4)
        val event1 = me(3);

        event1.eid should be("ME_APP_USAGE_SUMMARY");
//        event1.mid should be("0CB90A5476EFA88090619D48716786F7");
        event1.context.pdata.model.get should be("AppUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170504);
        event1.dimensions.author_id.get should be("all");
//        event1.dimensions.pdata.get.id should be("EkstepPortal");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(1);
        summary1.anon_total_ts should be(327.0);
        summary1.anon_total_sessions should be(1);
        summary1.anon_avg_ts_session should be(327.0);
        summary1.percent_new_users_count should be(50.0);
        summary1.ce_percent_sessions should be(33.33);
        summary1.ce_total_sessions should be(1);
        summary1.total_sessions should be(3);
        summary1.avg_ts_session should be(537.33);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1612.0);
        summary1.unique_users_count should be(2);
        summary1.unique_users.size should be(2);
        summary1.total_pageviews_count should be(45);

        // check for all anon sessions
        val event2 = me(0);

        event2.eid should be("ME_APP_USAGE_SUMMARY");
//        event2.mid should be("1F76DDAE997A41500066C4DB2914889B");
        event2.context.pdata.model.get should be("AppUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.period.get should be(20170503);
        event2.dimensions.author_id.get should be("all");
//        event2.dimensions.pdata.get.id should be("EkstepPortal");

        val summary2 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event2.edata.eks));
        summary2.new_user_count should be(0);
        summary2.anon_total_ts should be(1939.0);
        summary2.anon_total_sessions should be(4);
        summary2.anon_avg_ts_session should be(484.75);
        summary2.percent_new_users_count should be(0.0);
        summary2.ce_percent_sessions should be(0.0);
        summary2.ce_total_sessions should be(0);
        summary2.total_sessions should be(0);
        summary2.avg_ts_session should be(0.0);
        summary2.avg_pageviews should be(0.0);
        summary2.total_ts should be(0.0);
        summary2.unique_users_count should be(0);
        summary2.unique_users.size should be(0);
        summary2.total_pageviews_count should be(0);

        // check for specific author
        val event3 = me(1);

        event3.eid should be("ME_APP_USAGE_SUMMARY");
//        event3.mid should be("06B48BD1551B6BC71257C22955FD542F");
        event3.context.pdata.model.get should be("AppUsageSummarizer");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("DAY");
        event3.context.date_range should not be null;
        event3.dimensions.period.get should be(20170504);
        event3.dimensions.author_id.get should be("0313e644f8fda754eeeddc6c00eb824b00fea515");
//        event3.dimensions.pdata.get.id should be("EkstepPortal");

        val summary3 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event3.edata.eks));
        summary3.new_user_count should be(0);
        summary3.anon_total_ts should be(0.0);
        summary3.anon_total_sessions should be(0);
        summary3.anon_avg_ts_session should be(0.0);
        summary3.percent_new_users_count should be(0.0);
        summary3.ce_percent_sessions should be(50.0);
        summary3.ce_total_sessions should be(1);
        summary3.total_sessions should be(2);
        summary3.avg_ts_session should be(692.0);
        summary3.avg_pageviews should be(0.0);
        summary3.total_ts should be(1384.0);
        summary3.unique_users_count should be(0);
        summary3.unique_users.size should be(0);
        summary3.total_pageviews_count should be(0);
    }

    it should "generate 3 portal usage summary event where ce sessions count = 0" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_2.log");
        val rdd2 = AppUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(3)
        val event1 = me(2);

        event1.eid should be("ME_APP_USAGE_SUMMARY");
//        event1.mid should be("0BAEF1A3D5E41AF14C1428F78885F2E0");
        event1.context.pdata.model.get should be("AppUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170505);
        event1.dimensions.author_id.get should be("all");
//        event1.dimensions.pdata.get.id should be("EkstepPortal");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(1);
        summary1.anon_total_ts should be(794.0);
        summary1.anon_total_sessions should be(2);
        summary1.anon_avg_ts_session should be(397.0);
        summary1.percent_new_users_count should be(50.0);
        summary1.ce_percent_sessions should be(0.0);
        summary1.ce_total_sessions should be(0);
        summary1.total_sessions should be(2);
        summary1.avg_ts_session should be(572.5);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1145.0);
        summary1.unique_users_count should be(2);
        summary1.unique_users.size should be(2);
        summary1.total_pageviews_count should be(30);

        // check for specific author
        val event2 = me(0);

        event2.eid should be("ME_APP_USAGE_SUMMARY");
//        event2.mid should be("ED53E38023A115B137F9457748C0B181");
        event2.context.pdata.model.get should be("AppUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.period.get should be(20170505);
        event2.dimensions.author_id.get should be("0313e644f8fda754eeeddc6c00eb824b00fea515");
//        event2.dimensions.pdata.get.id should be("EkstepPortal");

        val summary2 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event2.edata.eks));
        summary2.new_user_count should be(0);
        summary2.anon_total_ts should be(0.0);
        summary2.anon_total_sessions should be(0);
        summary2.anon_avg_ts_session should be(0.0);
        summary2.percent_new_users_count should be(0.0);
        summary2.ce_percent_sessions should be(0.0);
        summary2.ce_total_sessions should be(0);
        summary2.total_sessions should be(1);
        summary2.avg_ts_session should be(917.0);
        summary2.avg_pageviews should be(0.0);
        summary2.total_ts should be(917.0);
        summary2.unique_users_count should be(0);
        summary2.unique_users.size should be(0);
        summary2.total_pageviews_count should be(0);
    }

    it should "generate 5 portal usage summary event where all are new visits" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/portal-usage-summary/test_data_3.log");
        val rdd2 = AppUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(5)
        val event1 = me(1);

        event1.eid should be("ME_APP_USAGE_SUMMARY");
//        event1.mid should be("49D2EBD068483F528EA4801EE2D1EC1A");
        event1.context.pdata.model.get should be("AppUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170502);
        event1.dimensions.author_id.get should be("all");
//        event1.dimensions.pdata.get.id should be("EkstepPortal");

        val summary1 = JSONUtils.deserialize[PortalUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.new_user_count should be(4);
        summary1.anon_total_ts should be(0.0);
        summary1.anon_total_sessions should be(0);
        summary1.anon_avg_ts_session should be(0.0);
        summary1.percent_new_users_count should be(100.0);
        summary1.ce_percent_sessions should be(0.0);
        summary1.ce_total_sessions should be(0);
        summary1.total_sessions should be(4);
        summary1.avg_ts_session should be(484.75);
        summary1.avg_pageviews should be(15.0);
        summary1.total_ts should be(1939.0);
        summary1.unique_users_count should be(4);
        summary1.unique_users.size should be(4);
        summary1.total_pageviews_count should be(60);
    }
}