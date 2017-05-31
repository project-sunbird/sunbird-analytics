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

        val rdd1 = loadFile[DerivedEvent]("src/test/resources/textbook-session-summary/textbook-session-summary1.log");
        val rdd2 = TextbookUsageSummaryModel.execute(rdd1, None);
        rdd2.foreach { x => println(JSONUtils.serialize(x)) }
        val me = rdd2.collect();
    }
}