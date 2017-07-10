package org.ekstep.analytics.model

import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DerivedEvent
/**
 * @author yuva
 */

class TestTextbookUsageSummaryModel extends SparkSpec(null) {

    "TextbookUsageSummaryModel" should "generate usage summary events" in {

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/textbook-usage-summary/textbook-session-summary2.log");
        val rdd2 = TextbookUsageSummaryModel.execute(rdd1, None);
        val me = rdd2.collect();
        me.length should be(4)
        val event1 = me(3);
        event1.eid should be("ME_TEXTBOOK_USAGE_SUMMARY");
//        event1.mid should be("5EC0DED34AEB97221856EA3D1628A71A");
        event1.context.pdata.model.get should be("TextbookUsageSummarizer");
        event1.context.pdata.ver should be("1.0");
        event1.context.granularity should be("DAY");
        event1.context.date_range should not be null;
        event1.dimensions.period.get should be(20170528);

        val summary1 = JSONUtils.deserialize[TextbookUsageOutput](JSONUtils.serialize(event1.edata.eks));
        summary1.unit_summary.added_count should be(6);
        summary1.unit_summary.deleted_count should be(3);
        summary1.unit_summary.modified_count should be(3);
        summary1.lesson_summary.added_count should be(6);
        summary1.lesson_summary.deleted_count should be(3);
        summary1.lesson_summary.modified_count should be(3);

    }
}