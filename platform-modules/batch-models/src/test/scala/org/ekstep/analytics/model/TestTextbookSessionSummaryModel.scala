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

    "TextbookSessionSummaryModel" should "generate start_time" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("start_time").get.asInstanceOf[Long] should be(1494843012334L)
    }

    it should "generate end_time" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("end_time").get.asInstanceOf[Long] should be(1494845088102L)
    }

    it should "generate time_spent" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("time_spent").get.asInstanceOf[Double] should be(2075.77)
    }

    it should "generate count for total_units_added" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("unit_summary").get.asInstanceOf[UnitSummary].total_units_added should be(1)
    }

    it should "generate count for total_units_deleted" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("unit_summary").get.asInstanceOf[UnitSummary].total_units_deleted should be(0)
    }

    it should "generate count for total_sub_units_added" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_sub_units_added should be(1)
    }

    it should "generate count for total_lessons_added" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_lessons_added should be(1)
    }

    it should "generate count for total_lessons_deleted" in {
        val rdd3 = computeCreationEvent(0)
        val metrics = rdd3.edata.eks.asInstanceOf[Map[String, AnyRef]]
        metrics.get("sub_unit_summary").get.asInstanceOf[SubUnitSummary].total_lessons_deleted should be(0)
    }

    private def computeCreationEvent: Array[MeasuredEvent] = {
        val rdd = loadFile[CreationEvent]("src/test/resources/textbook-session-summary/textbook-session-summary.log");
        val rdd2 = TextbookSessionSummaryModel.execute(rdd, None);
        rdd2.collect()
    }
}