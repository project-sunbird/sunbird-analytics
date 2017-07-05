/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestContentEditorUsageSummaryModel extends SparkSpec(null) {

    "ContentEditorUsageSummaryModel" should "generate 4 content editor usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-summary/test_data1.log");
        val rdd2 = ContentEditorUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(4)
        val event1 = me(0);

        event1.eid should be("ME_CE_USAGE_SUMMARY");
//        event1.mid should be("E9A898F7B82D74534CBEAA716AB5133F");
        event1.context.pdata.model.get should be("ContentEditorUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170522);
        event1.dimensions.content_id.get should be("all");

        val summary1 = JSONUtils.deserialize[CEUsageMetricsSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.unique_users_count should be(2);
        summary1.total_sessions should be(5);
        summary1.total_ts should be(84.04);
        summary1.avg_ts_session should be(16.81);

        // check for specific content_id
        val event2 = me(1);

        event2.eid should be("ME_CE_USAGE_SUMMARY");
//        event2.mid should be("9AFAAA707872BB96A049A6A7B0C53217");
        event2.context.pdata.model.get should be("ContentEditorUsageSummarizer");
        event2.context.pdata.ver should be("1.0");
        event2.context.granularity should be("DAY");
        event2.context.date_range should not be null;
        event2.dimensions.period.get should be(20170522);
        event2.dimensions.content_id.get should be("do_2122315986551685121193");

        val summary2 = JSONUtils.deserialize[CEUsageMetricsSummary](JSONUtils.serialize(event2.edata.eks));
        summary2.unique_users_count should be(1);
        summary2.total_sessions should be(2);
        summary2.total_ts should be(13.600000000000001);
        summary2.avg_ts_session should be(6.8);

        // check for specific content_id
        val event3 = me(2);

        event3.eid should be("ME_CE_USAGE_SUMMARY");
//        event3.mid should be("DD094BEE6CEE287BEE587E6BE6EC4BB2");
        event3.context.pdata.model.get should be("ContentEditorUsageSummarizer");
        event3.context.pdata.ver should be("1.0");
        event3.context.granularity should be("DAY");
        event3.context.date_range should not be null;
        event3.dimensions.period.get should be(20170522);
        event3.dimensions.content_id.get should be("do_2122217361752064001287");

        val summary3 = JSONUtils.deserialize[CEUsageMetricsSummary](JSONUtils.serialize(event3.edata.eks));
        summary3.unique_users_count should be(1);
        summary3.total_sessions should be(1);
        summary3.total_ts should be(0.72);
        summary3.avg_ts_session should be(0.72);
    }

    it should "generate only content editor usage summary event when content_id is empty" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-summary/test_data2.log");
        val rdd2 = ContentEditorUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();
        me.length should be(1)
        val event1 = me(0);
        event1.eid should be("ME_CE_USAGE_SUMMARY");
//        event1.mid should be("E9A898F7B82D74534CBEAA716AB5133F");
        event1.context.pdata.model.get should be("ContentEditorUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170522);
        event1.dimensions.content_id.get should be("all");

        val summary1 = JSONUtils.deserialize[CEUsageMetricsSummary](JSONUtils.serialize(event1.edata.eks));
        summary1.unique_users_count should be(2);
        summary1.total_sessions should be(5);
        summary1.total_ts should be(84.04);
        summary1.avg_ts_session should be(16.81);
    }
    
    it should "generate only content editor usage summary event" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/content-editor-usage-summary/test_data3.log");
        val rdd2 = ContentEditorUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();

        me.length should be(29)
    }
}