package org.ekstep.analytics.model

import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
/**
 * @author yuva
 */
class TestTextbookSessionSummaryModel extends SparkSpec(null) {

    "TextbookSessionSummaryModel" should "compute  session metrics if env data as textbook is present" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("start_time").get.asInstanceOf[Number].longValue() should be(1494843012334L)
        metrics.get("end_time").get.asInstanceOf[Number].longValue() should be(1494845088102L)
        metrics.get("time_spent").get.asInstanceOf[Number].doubleValue() should be(2075.77)
        metrics.get("unit_summary").get.asInstanceOf[UnitSummary].total_units_added should be(1)
        metrics.get("unit_summary").get.asInstanceOf[UnitSummary].total_units_deleted should be(0)
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_sub_units_added should be(1)
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_lessons_added should be(1)
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_lessons_deleted should be(0)
    }

    it should "generate empty results if env data as textbook is not present" in {
        val rdd = loadFile[CreationEvent]("src/test/resources/portal-session-summary/test_data_1.log");
        val rdd2 = TextbookSessionSummaryModel.execute(rdd, None);
        rdd2.isEmpty() should be(true)
    }

    private def computeCreationEvent: Array[MeasuredEvent] = {
        val rdd = loadFile[CreationEvent]("src/test/resources/textbook-session-summary/textbook-session-summary.log");
        val rdd2 = TextbookSessionSummaryModel.execute(rdd, None);
        rdd2.collect()
    }
}